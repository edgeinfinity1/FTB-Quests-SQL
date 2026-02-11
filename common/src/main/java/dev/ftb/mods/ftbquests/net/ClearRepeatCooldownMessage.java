package dev.ftb.mods.ftbquests.net;

import dev.architectury.networking.NetworkManager;
import dev.architectury.networking.simple.BaseS2CMessage;
import dev.architectury.networking.simple.MessageType;
import dev.ftb.mods.ftbquests.client.ClientQuestFile;
import dev.ftb.mods.ftbquests.quest.Quest;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.server.MinecraftServer;

public class ClearRepeatCooldownMessage extends BaseS2CMessage {
	private final long id;

    public ClearRepeatCooldownMessage(long id) {
        this.id = id;
    }

	public ClearRepeatCooldownMessage(FriendlyByteBuf buf) {
		this.id = buf.readLong();
	}

	public static void sendToAll(MinecraftServer server, Quest quest) {
		new ClearRepeatCooldownMessage(quest.id).sendToAll(server);
	}

	@Override
	public MessageType getType() {
		return FTBQuestsNetHandler.CLEAR_REPEAT_COOLDOWN;
	}

	@Override
	public void write(FriendlyByteBuf buf) {
		buf.writeLong(id);
	}

	@Override
	public void handle(NetworkManager.PacketContext context) {
		context.queue(() -> {
			if (ClientQuestFile.exists() && ClientQuestFile.INSTANCE.getBase(id) instanceof Quest quest) {
				ClientQuestFile.INSTANCE.selfTeamData.clearRepeatCooldown(quest);
			}
		});
	}
}