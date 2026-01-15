package dev.ftb.mods.ftbquests.client.neoforge;

import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.neoforge.client.event.ModelEvent;

public class ModelBakeEventHandler {
    private ModelBakeEventHandler() {}

    @SubscribeEvent
    public static void onModelBake(ModelEvent.ModifyBakingResult event) {
        // TODO: @since 21.11: Come back to this
//        override(event, ModBlocks.BARRIER.get(), CamouflagingModel::new);
//        override(event, ModBlocks.STAGE_BARRIER.get(), CamouflagingModel::new);
    }
// TODO: @since 21.11: Come back to this
//    private static void override(ModelEvent.ModifyBakingResult event, Block block, Function<BakedModel, CamouflagingModel> f) {
//        for (BlockState state : block.getStateDefinition().getPossibleStates()) {
//            ModelResourceLocation loc = BlockModelShaper.stateToModelLocation(state);
//            BakedModel model = event.getModels().get(loc);
//            if (model != null) {
//                event.getModels().put(loc, f.apply(model));
//            }
//        }
//    }
}
