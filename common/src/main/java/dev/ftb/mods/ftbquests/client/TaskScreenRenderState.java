package dev.ftb.mods.ftbquests.client;

import net.minecraft.client.renderer.blockentity.state.BlockEntityRenderState;
import net.minecraft.world.item.ItemStack;

import dev.ftb.mods.ftbquests.quest.task.Task;

import java.util.UUID;
import org.jspecify.annotations.Nullable;

public class TaskScreenRenderState extends BlockEntityRenderState {
    UUID teamId;
    Task task;
    boolean isInputOnly;
    ItemStack inputIcon;
    float @Nullable [] fakeTextureUV = null;
    boolean textHasShadow;
}
