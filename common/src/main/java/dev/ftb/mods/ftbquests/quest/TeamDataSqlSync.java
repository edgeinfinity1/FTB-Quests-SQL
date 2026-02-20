package dev.ftb.mods.ftbquests.quest;

import dev.ftb.mods.ftblibrary.snbt.SNBTCompoundTag;
import dev.ftb.mods.ftbquests.FTBQuests;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.TagParser;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Lightweight SQL snapshot synchronizer for TeamData.
 *
 * <p>Design goal: keep TeamData runtime logic in-memory and synchronize full snapshots via SQL.
 * This intentionally avoids complex mutation/event pipelines.</p>
 */
public class TeamDataSqlSync {
	private static final String PROP_PREFIX = "ftbquests.sync.mysql.";

	private final ServerQuestFile file;
	private final boolean enabled;
	private final String jdbcUrl;
	private final String username;
	private final String password;
	private final String tableName;
	private final String instanceId;
	private final long pollIntervalMs;

	private long lastPollAt;
	private long lastSeenUpdatedAt;
	private String lastSeenTeamId;
	private boolean tableEnsured;

	public TeamDataSqlSync(ServerQuestFile file) {
		this.file = file;
		enabled = Boolean.parseBoolean(System.getProperty(PROP_PREFIX + "enabled", "false"));
		jdbcUrl = System.getProperty(PROP_PREFIX + "url", "");
		username = System.getProperty(PROP_PREFIX + "user", "");
		password = System.getProperty(PROP_PREFIX + "password", "");
		tableName = sanitizeTableName(System.getProperty(PROP_PREFIX + "table", "ftbquests_team_data"));
		instanceId = System.getProperty(PROP_PREFIX + "instance_id", UUID.randomUUID().toString());
		pollIntervalMs = Long.parseLong(System.getProperty(PROP_PREFIX + "poll_interval_ms", "1000"));
		lastPollAt = 0L;
		lastSeenUpdatedAt = 0L;
		lastSeenTeamId = "";
		tableEnsured = false;
	}

	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Try to load all snapshots from SQL into memory.
	 *
	 * @return true if SQL load was attempted and at least one row was loaded; false means caller should fallback.
	 */
	public boolean loadFromSql() {
		if (!enabled || jdbcUrl.isBlank()) {
			return false;
		}

		ensureTable();

		int loaded = 0;
		try (Connection conn = openConnection();
			 PreparedStatement ps = conn.prepareStatement("SELECT team_id, payload, updated_at FROM " + tableName);
			 ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				String teamId = rs.getString(1);
				String payload = rs.getString(2);
				long updatedAt = rs.getLong(3);
				try {
					UUID uuid = UUID.fromString(teamId);
					CompoundTag nbt = TagParser.parseTag(payload);
					TeamData data = new TeamData(uuid, file);
					file.addData(data, true);
					data.deserializeNBT(SNBTCompoundTag.of(nbt));
					loaded++;
					advanceCursor(updatedAt, teamId);
				} catch (Exception ex) {
					FTBQuests.LOGGER.error("Failed to load TeamData snapshot from SQL for team {}: {}", teamId, ex.getMessage());
				}
			}
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("Failed to load TeamData snapshots from SQL: {}", ex.getMessage());
			return false;
		}

		if (loaded > 0) {
			FTBQuests.LOGGER.info("Loaded {} TeamData snapshots from SQL table {}", loaded, tableName);
		}
		return loaded > 0;
	}

	public void pushSnapshot(TeamData data) {
		if (!enabled || jdbcUrl.isBlank()) {
			return;
		}

		ensureTable();

		long now = System.currentTimeMillis();
		String teamId = data.getTeamId().toString();
		String payload = data.serializeNBT().toString();

		String sql = "INSERT INTO " + tableName + " (team_id, updated_at, source_instance, payload) VALUES (?, ?, ?, ?) " +
				"ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at), source_instance=VALUES(source_instance), payload=VALUES(payload)";

		try (Connection conn = openConnection();
			 PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, teamId);
			ps.setLong(2, now);
			ps.setString(3, instanceId);
			ps.setString(4, payload);
			ps.executeUpdate();
			advanceCursor(now, teamId);
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("Failed to upsert TeamData snapshot for team {}: {}", teamId, ex.getMessage());
		}
	}

	public void tick() {
		if (!enabled || jdbcUrl.isBlank()) {
			return;
		}

		long now = System.currentTimeMillis();
		if (now - lastPollAt < pollIntervalMs) {
			return;
		}
		lastPollAt = now;

		ensureTable();

		String sql = "SELECT team_id, updated_at, source_instance, payload FROM " + tableName +
				" WHERE (updated_at > ?) OR (updated_at = ? AND team_id > ?) ORDER BY updated_at ASC, team_id ASC";

		List<RemoteSnapshot> snapshots = new ArrayList<>();
		try (Connection conn = openConnection();
			 PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setLong(1, lastSeenUpdatedAt);
			ps.setLong(2, lastSeenUpdatedAt);
			ps.setString(3, lastSeenTeamId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					snapshots.add(new RemoteSnapshot(rs.getString(1), rs.getLong(2), rs.getString(3), rs.getString(4)));
				}
			}
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("Failed polling TeamData snapshots from SQL: {}", ex.getMessage());
			return;
		}

		for (RemoteSnapshot snapshot : snapshots) {
			advanceCursor(snapshot.updatedAt, snapshot.teamId);
			if (instanceId.equals(snapshot.sourceInstance)) {
				continue;
			}
			applyRemoteSnapshot(snapshot);
		}
	}

	public void shutdown() {
		// no-op for now
	}

	private void applyRemoteSnapshot(RemoteSnapshot snapshot) {
		try {
			UUID teamId = UUID.fromString(snapshot.teamId);
			CompoundTag nbt = TagParser.parseTag(snapshot.payload);
			TeamData teamData = Optional.ofNullable(file.getNullableTeamData(teamId)).orElseGet(() -> {
				TeamData newData = new TeamData(teamId, file);
				file.addData(newData, true);
				return newData;
			});
			teamData.deserializeNBT(SNBTCompoundTag.of(nbt));
			teamData.clearCachedProgress();
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("Failed applying remote TeamData snapshot for team {}: {}", snapshot.teamId, ex.getMessage());
		}
	}

	private void ensureTable() {
		if (tableEnsured) {
			return;
		}

		String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
				"team_id VARCHAR(36) NOT NULL PRIMARY KEY," +
				"updated_at BIGINT NOT NULL," +
				"source_instance VARCHAR(64) NOT NULL," +
				"payload LONGTEXT NOT NULL" +
				")";
		try (Connection conn = openConnection(); Statement st = conn.createStatement()) {
			st.execute(sql);
			tableEnsured = true;
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("Failed ensuring TeamData SQL table {} exists: {}", tableName, ex.getMessage());
		}
	}

	private Connection openConnection() throws SQLException {
		if (username.isBlank()) {
			return DriverManager.getConnection(jdbcUrl);
		}
		return DriverManager.getConnection(jdbcUrl, username, password);
	}

	private void advanceCursor(long updatedAt, String teamId) {
		if (updatedAt > lastSeenUpdatedAt) {
			lastSeenUpdatedAt = updatedAt;
			lastSeenTeamId = teamId;
		} else if (updatedAt == lastSeenUpdatedAt && teamId.compareTo(lastSeenTeamId) > 0) {
			lastSeenTeamId = teamId;
		}
	}

	private String sanitizeTableName(String value) {
		String cleaned = value.replaceAll("[^A-Za-z0-9_]", "");
		return cleaned.isEmpty() ? "ftbquests_team_data" : cleaned;
	}

	private record RemoteSnapshot(String teamId, long updatedAt, String sourceInstance, String payload) {
	}
}
