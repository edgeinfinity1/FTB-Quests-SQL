package dev.ftb.mods.ftbquests.quest.sync;

import com.mojang.brigadier.exceptions.CommandSyntaxException;
// import com.mojang.util.UndashedUuid;
// import dev.ftb.mods.ftbquests.quest.sync.UndashedUuidCompat;
// import dev.architectury.networking.NetworkManager;
import dev.architectury.platform.Platform;
import dev.ftb.mods.ftblibrary.snbt.SNBT;
import dev.ftb.mods.ftblibrary.snbt.SNBTCompoundTag;
import dev.ftb.mods.ftbquests.FTBQuests;
import dev.ftb.mods.ftbquests.net.SyncTeamDataMessage;
import dev.ftb.mods.ftbquests.quest.ServerQuestFile;
import dev.ftb.mods.ftbquests.quest.TeamData;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NumericTag;
import net.minecraft.nbt.Tag;
import net.minecraft.nbt.TagParser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Optional MySQL synchronization for TeamData between multiple server instances.
 *
 * Configure with config/ftb_quests_sql.snbt (generated from template on first start).
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
	private static final String CONFIG_FILE_NAME = "ftb_quests_sql.snbt";

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

		TeamDataDelta delta = buildDelta(previous, snapshot);
		UUID teamId = teamData.getTeamId();
		enqueue(() -> pushSnapshot(teamId, snapshot, delta));
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

		primeFromDatabase();
	}

	private void primeFromDatabase() {
		String sql = """
				SELECT team_id, payload, updated_at
				FROM %s
				ORDER BY updated_at ASC
				""".formatted(tableName);

		long maxSeen = lastPollTimeMillis;
		int queued = 0;
		try (Connection c = openConnection(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				long updatedAt = rs.getLong("updated_at");
				if (updatedAt > maxSeen) {
					maxSeen = updatedAt;
				}

				UUID teamId = UndashedUuidCompat.fromString(rs.getString("team_id"));
				String payload = rs.getString("payload");
				String existing = snapshotCache.get(teamId);
				if (!payload.equals(existing)) {
					pendingRemoteUpdates.add(new RemoteTeamDataUpdate(teamId, payload));
					queued++;
				}
			}
			lastPollTimeMillis = maxSeen;
			if (queued > 0) {
				FTBQuests.LOGGER.info("Queued {} TeamData update(s) from SQL during startup prime", queued);
			}
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("TeamData MySQL startup prime failed: {}", ex.getMessage());
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

	private void pushSnapshot(UUID teamId, String snapshot, TeamDataDelta delta) {
		long now = System.currentTimeMillis();
		String teamKey = UndashedUuidCompat.toString(teamId);

		String selectSql = "SELECT payload FROM %s WHERE team_id = ? FOR UPDATE".formatted(tableName);
		String insertSql = "INSERT INTO %s (team_id, payload, updated_at, source_server) VALUES (?, ?, ?, ?)".formatted(tableName);
		String updateSql = "UPDATE %s SET payload = ?, updated_at = ?, source_server = ? WHERE team_id = ?".formatted(tableName);

		try (Connection c = openConnection()) {
			c.setAutoCommit(false);
			c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

			String payloadToStore = snapshot;
			boolean exists = false;
			try (PreparedStatement select = c.prepareStatement(selectSql)) {
				select.setString(1, teamKey);
				try (ResultSet rs = select.executeQuery()) {
					if (rs.next()) {
						exists = true;
						String dbPayload = rs.getString("payload");
						if (delta != null && dbPayload != null && !dbPayload.isEmpty()) {
							String merged = applyDelta(dbPayload, delta);
							if (merged != null) {
								payloadToStore = merged;
							}
						}
					}
				}
			}

			if (exists) {
				try (PreparedStatement update = c.prepareStatement(updateSql)) {
					update.setString(1, payloadToStore);
					update.setLong(2, now);
					update.setString(3, serverId);
					update.setString(4, teamKey);
					update.executeUpdate();
				}
			} else {
				try (PreparedStatement insert = c.prepareStatement(insertSql)) {
					insert.setString(1, teamKey);
					insert.setString(2, payloadToStore);
					insert.setLong(3, now);
					insert.setString(4, serverId);
					insert.executeUpdate();
				} catch (SQLIntegrityConstraintViolationException ignored) {
					// concurrent first-writer raced us; apply merged payload via update instead
					try (PreparedStatement update = c.prepareStatement(updateSql)) {
						update.setString(1, payloadToStore);
						update.setLong(2, now);
						update.setString(3, serverId);
						update.setString(4, teamKey);
						update.executeUpdate();
					}
				}
			}

			c.commit();
		} catch (Exception ex) {
			FTBQuests.LOGGER.error("TeamData MySQL push failed for {}: {}", teamId, ex.getMessage());
		}
	}

	private TeamDataDelta buildDelta(String previousSnapshot, String currentSnapshot) {
		if (previousSnapshot == null || previousSnapshot.isEmpty()) {
			return null;
		}

		try {
			Tag previousTag = TagParser.parseTag(previousSnapshot);
			Tag currentTag = TagParser.parseTag(currentSnapshot);
			if (!(previousTag instanceof CompoundTag previous) || !(currentTag instanceof CompoundTag current)) {
				return null;
			}

			List<DeltaOperation> operations = new ArrayList<>();
			if (!appendCompoundDiff(previous, current, new ArrayList<>(), operations)) {
				return null;
			}

			return operations.isEmpty() ? null : new TeamDataDelta(operations);
		} catch (CommandSyntaxException ex) {
			return null;
		}
	}

	private String applyDelta(String payload, TeamDataDelta delta) {
		try {
			Tag parsed = TagParser.parseTag(payload);
			if (!(parsed instanceof CompoundTag target)) {
				return null;
			}

			for (DeltaOperation operation : delta.operations()) {
				if (!applyOperation(target, operation)) {
					return null;
				}
			}

			return target.toString();
		} catch (CommandSyntaxException ex) {
			return null;
		}
	}

	private boolean appendCompoundDiff(CompoundTag previous, CompoundTag current, List<String> path, List<DeltaOperation> out) {
		Set<String> keys = new LinkedHashSet<>(previous.getAllKeys());
		keys.addAll(current.getAllKeys());

		for (String key : keys) {
			boolean hadPrevious = previous.contains(key);
			boolean hasCurrent = current.contains(key);
			List<String> nextPath = appendPath(path, key);

			if (!hadPrevious) {
				out.add(DeltaOperation.put(nextPath, current.get(key).copy()));
				continue;
			}

			if (!hasCurrent) {
				out.add(DeltaOperation.remove(nextPath));
				continue;
			}

			Tag prevValue = previous.get(key);
			Tag currValue = current.get(key);
			if (prevValue.equals(currValue)) {
				continue;
			}

			if (prevValue instanceof CompoundTag prevCompound && currValue instanceof CompoundTag currCompound) {
				if (!appendCompoundDiff(prevCompound, currCompound, nextPath, out)) {
					return false;
				}
				continue;
			}

			if (isBooleanLike(prevValue) && isBooleanLike(currValue)) {
				out.add(DeltaOperation.boolSet(nextPath, ((NumericTag) currValue).getAsByte() != 0));
				continue;
			}

			if (prevValue instanceof NumericTag prevNumber && currValue instanceof NumericTag currNumber) {
				if (isFloatingType(prevValue) || isFloatingType(currValue)) {
					double delta = currNumber.getAsDouble() - prevNumber.getAsDouble();
					out.add(DeltaOperation.addDouble(nextPath, delta));
				} else {
					long delta = currNumber.getAsLong() - prevNumber.getAsLong();
					out.add(DeltaOperation.addLong(nextPath, delta));
				}
				continue;
			}

			return false;
		}

		return true;
	}

	private boolean applyOperation(CompoundTag root, DeltaOperation op) {
		List<String> path = op.path();
		if (path.isEmpty()) {
			return false;
		}

		CompoundTag parent = getOrCreateParent(root, path, op.kind() == DeltaKind.PUT || op.kind() == DeltaKind.BOOL_SET);
		if (parent == null) {
			return false;
		}

		String key = path.get(path.size() - 1);
		switch (op.kind()) {
			case PUT -> parent.put(key, op.value().copy());
			case REMOVE -> parent.remove(key);
			case BOOL_SET -> parent.putBoolean(key, op.boolValue());
			case ADD_LONG -> {
				Tag existing = parent.get(key);
				if (!(existing instanceof NumericTag number) || isFloatingType(existing)) {
					return false;
				}
				parent.putLong(key, number.getAsLong() + op.longDelta());
			}
			case ADD_DOUBLE -> {
				Tag existing = parent.get(key);
				if (!(existing instanceof NumericTag number)) {
					return false;
				}
				parent.putDouble(key, number.getAsDouble() + op.doubleDelta());
			}
		}

		return true;
	}

	private CompoundTag getOrCreateParent(CompoundTag root, List<String> path, boolean create) {
		CompoundTag cursor = root;
		for (int i = 0; i < path.size() - 1; i++) {
			String step = path.get(i);
			Tag current = cursor.get(step);
			if (current instanceof CompoundTag compound) {
				cursor = compound;
				continue;
			}

			if (!create) {
				return null;
			}

			CompoundTag next = new CompoundTag();
			cursor.put(step, next);
			cursor = next;
		}

		return cursor;
	}

	private List<String> appendPath(List<String> basePath, String key) {
		List<String> path = new ArrayList<>(basePath);
		path.add(key);
		return path;
	}

	private boolean isBooleanLike(Tag value) {
		return value instanceof NumericTag number && value.getId() == Tag.TAG_BYTE && (number.getAsByte() == 0 || number.getAsByte() == 1);
	}

	private boolean isFloatingType(Tag value) {
		return value.getId() == Tag.TAG_FLOAT || value.getId() == Tag.TAG_DOUBLE;
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

	private record TeamDataDelta(List<DeltaOperation> operations) {
	}

	private enum DeltaKind {
		PUT,
		REMOVE,
		BOOL_SET,
		ADD_LONG,
		ADD_DOUBLE
	}

	private record DeltaOperation(DeltaKind kind, List<String> path, Tag value, boolean boolValue, long longDelta, double doubleDelta) {
		private static DeltaOperation put(List<String> path, Tag value) {
			return new DeltaOperation(DeltaKind.PUT, path, value, false, 0L, 0D);
		}

		private static DeltaOperation remove(List<String> path) {
			return new DeltaOperation(DeltaKind.REMOVE, path, null, false, 0L, 0D);
		}

		private static DeltaOperation boolSet(List<String> path, boolean value) {
			return new DeltaOperation(DeltaKind.BOOL_SET, path, null, value, 0L, 0D);
		}

		private static DeltaOperation addLong(List<String> path, long delta) {
			return new DeltaOperation(DeltaKind.ADD_LONG, path, null, false, delta, 0D);
		}

		private static DeltaOperation addDouble(List<String> path, double delta) {
			return new DeltaOperation(DeltaKind.ADD_DOUBLE, path, null, false, 0L, delta);
		}
	}

	private record Config(boolean enabled, String jdbcUrl, String user, String password, String tableName, String serverId, int pollIntervalTicks) {
	}
}
