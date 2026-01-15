package dev.ftb.mods.ftbquests.block.entity;

import net.minecraft.world.item.ItemStack;

import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

public interface ITaskScreen extends IEditable {
    Optional<TaskScreenBlockEntity> getCoreScreen();

    @NotNull
    UUID getTeamId();

    boolean isInputOnly();

    boolean isIndestructible();

    ItemStack getSkin();
}
