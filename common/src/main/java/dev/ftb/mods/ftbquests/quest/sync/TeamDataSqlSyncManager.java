package dev.ftb.mods.ftbquests.quest.sync;

import com.mojang.brigadier.exceptions.CommandSyntaxException;
// import com.mojang.util.UndashedUuid;
import dev.ftb.mods.ftbquests.quest.sync.UndashedUuidCompat;
// import dev.architectury.networking.NetworkManager;
import dev.architectury.platform.Platform;
import dev.ftb.mods.ftblibrary.snbt.SNBT;
import dev.ftb.mods.ftblibrary.snbt.SNBTCompoundTag;
import dev.ftb.mods.ftbquests.FTBQuests;
import dev.ftb.mods.ftbquests.net.SyncTeamDataMessage;
import dev.ftb.mods.ftbquests.quest.ServerQuestFile;
import dev.ftb.mods.ftbquests.quest.TeamData;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.TagParser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Optional MySQL synchronization for TeamData between multiple server instances.
 *
 * Configure with config/team_data_sql_sync.snbt (generated from template on first start).
 * JVM properties are still supported as overrides.
 */
public enum TeamDataSqlSyncManager {
	INSTANCE;

	private static final String KEY_ENABLED = "ftbquests.sync.mysql.enabled";
	private static final String KEY_URL = "ftbquests.sync.mysql.url";
	private static final String KEY_USER = "ftbquests.sync.mysql.user";
	private static final String KEY_PASSWORD = "ftbquests.sync.mysql.password";
	private static final String KEY_TABLE = "ftbquests.sync.mysql.table";
	private static final String KEY_SERVER_ID = "ftbquests.sync.mysql.server_id";
	private static final String KEY_POLL_TICKS = "ftbquests.sync.mysql.poll_interval_ticks";

	private static final String DEFAULT_TABLE = "ftbq_team_data_sync";
	private static final int DEFAULT_POLL_TICKS = 40;
	private static final String CONFIG_FILE_NAME = "team_data_sql_sync.snbt";

	private ExecutorService executor;
	private final Queue<RemoteTeamDataUpdate> pendingRemoteUpdates = new ConcurrentLinkedQueue<>();
	private final ConcurrentMap<UUID, String> snapshotCache = new ConcurrentHashMap<>();

	private volatile boolean enabled;
	private volatile String jdbcUrl;
	private volatile String user;
	private volatile String password;
	private volatile String tableName;
	private volatile String serverId;
	private volatile int pollIntervalTicks;
	private volatile long lastPollTimeMillis;
	private volatile boolean pollInFlight;
	private volatile Config config;

	public void start(ServerQuestFile file) {
		config = loadConfig(file);
		enabled = config.enabled();
		if (!enabled) {
			return;
		}

		jdbcUrl = config.jdbcUrl();
		user = config.user();
		password = config.password();
		tableName = config.tableName();
		serverId = config.serverId();
		pollIntervalTicks = config.pollIntervalTicks();
		if (pollIntervalTicks <= 0) pollIntervalTicks = DEFAULT_POLL_TICKS;

		if (jdbcUrl.isEmpty() || user.isEmpty()) {
			enabled = false;
			FTBQuests.LOGGER.error("TeamData MySQL sync disabled: missing {} or {}", KEY_URL, KEY_USER);
			return;
		}

		if (serverId.isEmpty()) {
			serverId = file.server.getMotd() + "-" + file.server.getPort();
		}

		executor = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "FTBQuests-TeamData-SQL-Sync");
			t.setDaemon(true);
			return t;
		});

		lastPollTimeMillis = System.currentTimeMillis();
		snapshotCache.clear();
		pendingRemoteUpdates.clear();
		pollInFlight = false;

		enqueue(() -> initializeSchemaAndPrimeCache(file));
		FTBQuests.LOGGER.info("TeamData MySQL sync enabled; polling every {} ticks", pollIntervalTicks);
	}

	public void stop() {
		enabled = false;
		config = null;
		pollInFlight = false;
		pendingRemoteUpdates.clear();
		snapshotCache.clear();
		if (executor != null) {
			executor.shutdownNow();
			executor = null;
		}
	}

	public void tick(ServerQuestFile file) {
		if (!enabled) {
			return;
		}

		for (RemoteTeamDataUpdate update; (update = pendingRemoteUpdates.poll()) != null; ) {
			applyRemoteUpdate(file, update);
		}

		if (!pollInFlight && file.server.getTickCount() % pollIntervalTicks == 0) {
			pollInFlight = true;
			enqueue(() -> pollRemoteUpdates());
		}
	}

	public void onTeamDataDirty(ServerQuestFile file, TeamData teamData) {
		if (!enabled) {
			return;
		}

		String snapshot = teamData.serializeNBT().toString();
		String previous = snapshotCache.put(teamData.getTeamId(), snapshot);
		if (snapshot.equals(previous)) {
			return;
		}

		UUID teamId = teamData.getTeamId();
		enqueue(() -> pushSnapshot(teamId, snapshot));
	}

	private void initializeSchemaAndPrimeCache(ServerQuestFile file) {
		try (Connection c = openConnection(); Statement s = c.createStatement()) {
			s.execute("""
					CREATE TABLE IF NOT EXISTS %s (
						team_id CHAR(32) NOT NULL PRIMARY KEY,
						payload LONGTEXT NOT NULL,
						updated_at BIGINT NOT NULL,
						source_server VARCHAR(128) NOT NULL
					)
					""".formatted(tableName));
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("TeamData MySQL sync schema init failed: {}", ex.getMessage());
			return;
		}

		for (TeamData data : file.getAllTeamData()) {
			snapshotCache.put(data.getTeamId(), data.serializeNBT().toString());
		}
	}

	private Config loadConfig(ServerQuestFile ignoredFile) {
		Path configDir = Platform.getConfigFolder();
		Path configPath = configDir.resolve(CONFIG_FILE_NAME);

		if (Files.notExists(configPath)) {
			writeDefaultConfig(configPath);
		}

		CompoundTag loaded = SNBT.read(configPath);
		CompoundTag mysql = loaded == null ? new CompoundTag() : loaded.getCompound("mysql_sync");

		boolean enabled = mysql.getBoolean("enabled");
		String jdbcUrl = mysql.getString("url").trim();
		String user = mysql.getString("user").trim();
		String password = mysql.getString("password");
		String tableName = mysql.contains("table") ? mysql.getString("table").trim() : DEFAULT_TABLE;
		String serverId = mysql.getString("server_id").trim();
		int pollIntervalTicks = mysql.contains("poll_interval_ticks") ? mysql.getInt("poll_interval_ticks") : DEFAULT_POLL_TICKS;

		// compatibility: allow JVM property override when provided
		enabled = Boolean.parseBoolean(System.getProperty(KEY_ENABLED, String.valueOf(enabled)));
		jdbcUrl = System.getProperty(KEY_URL, jdbcUrl).trim();
		user = System.getProperty(KEY_USER, user).trim();
		password = System.getProperty(KEY_PASSWORD, password);
		tableName = System.getProperty(KEY_TABLE, tableName).trim();
		serverId = System.getProperty(KEY_SERVER_ID, serverId).trim();
		pollIntervalTicks = Integer.getInteger(KEY_POLL_TICKS, pollIntervalTicks);

		if (tableName.isEmpty()) {
			tableName = DEFAULT_TABLE;
		}

		FTBQuests.LOGGER.info("Loaded TeamData SQL sync config from {}", configPath);
		return new Config(enabled, jdbcUrl, user, password, tableName, serverId, pollIntervalTicks);
	}

	private void writeDefaultConfig(Path configPath) {
		try {
			Files.createDirectories(configPath.getParent());
			SNBTCompoundTag root = new SNBTCompoundTag();
			SNBTCompoundTag mysql = new SNBTCompoundTag();
			mysql.putBoolean("enabled", false);
			mysql.putString("url", "jdbc:mysql://127.0.0.1:3306/ftbquests");
			mysql.putString("user", "root");
			mysql.putString("password", "");
			mysql.putString("table", DEFAULT_TABLE);
			mysql.putString("server_id", "");
			mysql.putInt("poll_interval_ticks", DEFAULT_POLL_TICKS);
			root.put("mysql_sync", mysql);

			if (SNBT.write(configPath, root)) {
				FTBQuests.LOGGER.info("Created TeamData SQL sync template config: {}", configPath);
			}
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("Failed to create TeamData SQL sync config {}: {}", configPath, ex.getMessage());
		}
	}

	private void pushSnapshot(UUID teamId, String snapshot) {
		long now = System.currentTimeMillis();
		String sql = """
				INSERT INTO %s (team_id, payload, updated_at, source_server)
				VALUES (?, ?, ?, ?)
				ON DUPLICATE KEY UPDATE payload=VALUES(payload), updated_at=VALUES(updated_at), source_server=VALUES(source_server)
				""".formatted(tableName);

		try (Connection c = openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setString(1, UndashedUuidCompat.toString(teamId));
			ps.setString(2, snapshot);
			ps.setLong(3, now);
			ps.setString(4, serverId);
			ps.executeUpdate();
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("TeamData MySQL push failed for {}: {}", teamId, ex.getMessage());
		}
	}

	private void pollRemoteUpdates() {
		String sql = """
				SELECT team_id, payload, updated_at, source_server
				FROM %s
				WHERE updated_at > ?
				ORDER BY updated_at ASC
				""".formatted(tableName);

		long maxSeen = lastPollTimeMillis;

		try (Connection c = openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
			ps.setLong(1, lastPollTimeMillis);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					String source = rs.getString("source_server");
					long updatedAt = rs.getLong("updated_at");
					if (updatedAt > maxSeen) {
						maxSeen = updatedAt;
					}
					if (serverId.equals(source)) {
						continue;
					}

					UUID teamId = UndashedUuidCompat.fromString(rs.getString("team_id"));
					String payload = rs.getString("payload");
					String existing = snapshotCache.get(teamId);
					if (!payload.equals(existing)) {
						pendingRemoteUpdates.add(new RemoteTeamDataUpdate(teamId, payload));
					}
				}
			}

			lastPollTimeMillis = maxSeen;
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("TeamData MySQL poll failed: {}", ex.getMessage());
		} finally {
			pollInFlight = false;
		}
	}

	private void applyRemoteUpdate(ServerQuestFile file, RemoteTeamDataUpdate update) {
		try {
			CompoundTag tag = TagParser.parseTag(update.payload());
			TeamData teamData = file.getOrCreateTeamData(update.teamId());
			teamData.deserializeNBT(tag);
			teamData.clearCachedProgress();
			snapshotCache.put(update.teamId(), update.payload());

			if (!teamData.getOnlineMembers().isEmpty()) {
				new SyncTeamDataMessage(teamData, true).sendTo(teamData.getOnlineMembers());
			}
		} catch (CommandSyntaxException ex) {
			FTBQuests.LOGGER.error("TeamData MySQL payload parse failed for {}: {}", update.teamId(), ex.getMessage());
		}
	}

	private Connection openConnection() throws Exception {
		return DriverManager.getConnection(jdbcUrl, user, password);
	}

	private void enqueue(Runnable runnable) {
		ExecutorService ex = executor;
		if (enabled && ex != null) {
			try {
				ex.submit(runnable);
			} catch (RejectedExecutionException ignored) {
			}
		}
	}

	private record RemoteTeamDataUpdate(UUID teamId, String payload) {
	}

	private record Config(boolean enabled, String jdbcUrl, String user, String password, String tableName, String serverId, int pollIntervalTicks) {
	}
}
