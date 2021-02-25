package jadx.core.dex.visitors.blocksmaker;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jadx.core.dex.attributes.AFlag;
import jadx.core.dex.attributes.AType;
import jadx.core.dex.attributes.nodes.LoopInfo;
import jadx.core.dex.instructions.InsnType;
import jadx.core.dex.instructions.args.InsnArg;
import jadx.core.dex.instructions.args.RegisterArg;
import jadx.core.dex.nodes.BlockNode;
import jadx.core.dex.nodes.Edge;
import jadx.core.dex.nodes.InsnNode;
import jadx.core.dex.nodes.MethodNode;
import jadx.core.dex.trycatch.CatchAttr;
import jadx.core.dex.trycatch.ExcHandlerAttr;
import jadx.core.dex.trycatch.ExceptionHandler;
import jadx.core.dex.trycatch.TryCatchBlock;
import jadx.core.dex.visitors.AbstractVisitor;
import jadx.core.utils.BlockUtils;
import jadx.core.utils.exceptions.JadxOverflowException;
import jadx.core.utils.exceptions.JadxRuntimeException;
import static jadx.core.dex.visitors.blocksmaker.BlockSplitter.connect;
import static jadx.core.utils.EmptyBitSet.EMPTY;

public class BlockProcessor extends AbstractVisitor {

    private static final Logger LOG = LoggerFactory.getLogger(BlockProcessor.class);

    @Override
    public void visit(MethodNode mth) {
        if (mth.isNoCode()) {
            return;
        }
        processBlocksTree(mth);
    }

    public static void rerun(MethodNode mth) {
        removeMarkedBlocks(mth);
        clearBlocksState(mth);
        processBlocksTree(mth);
    }

    private static void processBlocksTree(MethodNode mth) {
        computeDominators(mth);
        if (independentBlockTreeMod(mth)) {
            clearBlocksState(mth);
            computeDominators(mth);
        }
        markReturnBlocks(mth);
        int i = 0;
        while (modifyBlocksTree(mth)) {
            clearBlocksState(mth);
            computeDominators(mth);
            markReturnBlocks(mth);
            if (i++ > 100) {
                mth.addWarn("CFG modification limit reached, blocks count: " + mth.getBasicBlocks().size());
                break;
            }
        }
        checkForUnreachableBlocks(mth);
        computeDominanceFrontier(mth);
        registerLoops(mth);
        processNestedLoops(mth);
    }

    private static void checkForUnreachableBlocks(MethodNode mth) {
        mth.getBasicBlocks().forEach(block -> {
            if (block.getPredecessors().isEmpty() && block != mth.getEnterBlock()) {
                throw new JadxRuntimeException("Unreachable block: " + block);
            }
        });
    }

    private static boolean canRemoveBlock(BlockNode block) {
        return block.getInstructions().isEmpty() && block.isAttrStorageEmpty() && block.getSuccessors().size() <= 1 && !block.getPredecessors().isEmpty();
    }

    private static boolean removeEmptyBlock(BlockNode block) {
        if (canRemoveBlock(block)) {
            if (block.getSuccessors().size() == 1) {
                BlockNode successor = block.getSuccessors().get(0);
                block.getPredecessors().forEach(pred -> {
                    pred.getSuccessors().remove(block);
                    BlockSplitter.connect(pred, successor);
                    BlockSplitter.replaceTarget(pred, block, successor);
                    pred.updateCleanSuccessors();
                });
                BlockSplitter.removeConnection(block, successor);
            } else {
                block.getPredecessors().forEach(pred -> {
                    pred.getSuccessors().remove(block);
                    pred.updateCleanSuccessors();
                });
            }
            block.add(AFlag.REMOVE);
            block.getSuccessors().clear();
            block.getPredecessors().clear();
            return true;
        }
        return false;
    }

    private static boolean deduplicateBlockInsns(BlockNode block) {
        if (block.contains(AFlag.LOOP_START) || block.contains(AFlag.LOOP_END)) {
            List<BlockNode> predecessors = block.getPredecessors();
            int predsCount = predecessors.size();
            if (predsCount > 1) {
                InsnNode lastInsn = BlockUtils.getLastInsn(block);
                if (lastInsn != null && lastInsn.getType() == InsnType.IF) {
                    return false;
                }
                int sameInsnCount = getSameLastInsnCount(predecessors);
                if (sameInsnCount > 0) {
                    List<InsnNode> insns = getLastInsns(predecessors.get(0), sameInsnCount);
                    insertAtStart(block, insns);
                    predecessors.forEach(pred -> getLastInsns(pred, sameInsnCount).clear());
                    LOG.debug("Move duplicate insns, count: {} to block {}", sameInsnCount, block);
                    return true;
                }
            }
        }
        return false;
    }

    private static List<InsnNode> getLastInsns(BlockNode blockNode, int sameInsnCount) {
        List<InsnNode> instructions = blockNode.getInstructions();
        int size = instructions.size();
        return instructions.subList(size - sameInsnCount, size);
    }

    private static void insertAtStart(BlockNode block, List<InsnNode> insns) {
        List<InsnNode> blockInsns = block.getInstructions();
        List<InsnNode> newInsnList = new ArrayList<>(insns.size() + blockInsns.size());
        newInsnList.addAll(insns);
        newInsnList.addAll(blockInsns);
        blockInsns.clear();
        blockInsns.addAll(newInsnList);
    }

    private static int getSameLastInsnCount(List<BlockNode> predecessors) {
        int sameInsnCount = 0;
        while (true) {
            InsnNode insn = null;
            for (BlockNode pred : predecessors) {
                InsnNode curInsn = getInsnsFromEnd(pred, sameInsnCount);
                if (curInsn == null) {
                    return sameInsnCount;
                }
                if (insn == null) {
                    insn = curInsn;
                } else {
                    if (!isSame(insn, curInsn)) {
                        return sameInsnCount;
                    }
                }
            }
            sameInsnCount++;
        }
    }

    private static boolean isSame(InsnNode insn, InsnNode curInsn) {
        return insn.isDeepEquals(curInsn) && insn.canReorder();
    }

    private static InsnNode getInsnsFromEnd(BlockNode block, int number) {
        List<InsnNode> instructions = block.getInstructions();
        int insnCount = instructions.size();
        if (insnCount <= number) {
            return null;
        }
        return instructions.get(insnCount - number - 1);
    }

    private static void computeDominators(MethodNode mth) {
        List<BlockNode> basicBlocks = mth.getBasicBlocks();
        int nBlocks = basicBlocks.size();
        for (int i = 0; i < nBlocks; i++) {
            BlockNode block = basicBlocks.get(i);
            block.setId(i);
            block.setDoms(new BitSet(nBlocks));
            block.getDoms().set(0, nBlocks);
        }
        BlockNode entryBlock = mth.getEnterBlock();
        calcDominators(basicBlocks, entryBlock);
        markLoops(mth);
        basicBlocks.forEach(block -> block.getDoms().clear(block.getId()));
        calcImmediateDominators(basicBlocks, entryBlock);
    }

    private static void calcDominators(List<BlockNode> basicBlocks, BlockNode entryBlock) {
        entryBlock.getDoms().clear();
        entryBlock.getDoms().set(entryBlock.getId());
        BitSet domSet = new BitSet(basicBlocks.size());
        boolean changed;
        do {
            changed = false;
            for (BlockNode block : basicBlocks) {
                if (block == entryBlock) {
                    continue;
                }
                BitSet d = block.getDoms();
                if (!changed) {
                    domSet.clear();
                    domSet.or(d);
                }
                for (BlockNode pred : block.getPredecessors()) {
                    d.and(pred.getDoms());
                }
                d.set(block.getId());
                if (!changed && !d.equals(domSet)) {
                    changed = true;
                }
            }
        } while (changed);
    }

    private static void calcImmediateDominators(List<BlockNode> basicBlocks, BlockNode entryBlock) {
        for (BlockNode block : basicBlocks) {
            if (block == entryBlock) {
                continue;
            }
            BlockNode idom;
            List<BlockNode> preds = block.getPredecessors();
            if (preds.size() == 1) {
                idom = preds.get(0);
            } else {
                BitSet bs = new BitSet(block.getDoms().length());
                bs.or(block.getDoms());
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
                    BlockNode dom = basicBlocks.get(i);
                    bs.andNot(dom.getDoms());
                }
                if (bs.cardinality() != 1) {
                    throw new JadxRuntimeException("Can't find immediate dominator for block " + block + " in " + bs + " preds:" + preds);
                }
                idom = basicBlocks.get(bs.nextSetBit(0));
            }
            block.setIDom(idom);
            idom.addDominatesOn(block);
        }
    }

    private static void computeDominanceFrontier(MethodNode mth) {
        for (BlockNode exit : mth.getExitBlocks()) {
            exit.setDomFrontier(EMPTY);
        }
        for (BlockNode block : mth.getBasicBlocks()) {
            try {
                computeBlockDF(mth, block);
            } catch (StackOverflowError e) {
                throw new JadxOverflowException("Failed compute block dominance frontier");
            } catch (Exception e) {
                throw new JadxRuntimeException("Failed compute block dominance frontier", e);
            }
        }
    }

    private static void computeBlockDF(MethodNode mth, BlockNode block) {
        if (block.getDomFrontier() != null) {
            return;
        }
        block.getDominatesOn().forEach(domBlock -> computeBlockDF(mth, domBlock));
        List<BlockNode> blocks = mth.getBasicBlocks();
        BitSet domFrontier = null;
        for (BlockNode s : block.getSuccessors()) {
            if (s.getIDom() != block) {
                if (domFrontier == null) {
                    domFrontier = new BitSet(blocks.size());
                }
                domFrontier.set(s.getId());
            }
        }
        for (BlockNode c : block.getDominatesOn()) {
            BitSet frontier = c.getDomFrontier();
            for (int p = frontier.nextSetBit(0); p >= 0; p = frontier.nextSetBit(p + 1)) {
                if (blocks.get(p).getIDom() != block) {
                    if (domFrontier == null) {
                        domFrontier = new BitSet(blocks.size());
                    }
                    domFrontier.set(p);
                }
            }
        }
        if (domFrontier == null || domFrontier.cardinality() == 0) {
            domFrontier = EMPTY;
        }
        block.setDomFrontier(domFrontier);
    }

    private static void markReturnBlocks(MethodNode mth) {
        mth.getExitBlocks().clear();
        mth.getBasicBlocks().forEach(block -> {
            if (BlockUtils.checkLastInsnType(block, InsnType.RETURN)) {
                block.add(AFlag.RETURN);
                mth.getExitBlocks().add(block);
            }
        });
    }

    private static void markLoops(MethodNode mth) {
        mth.getBasicBlocks().forEach(block -> {
            block.getSuccessors().forEach(successor -> {
                if (block.getDoms().get(successor.getId())) {
                    successor.add(AFlag.LOOP_START);
                    block.add(AFlag.LOOP_END);
                    LoopInfo loop = new LoopInfo(successor, block);
                    successor.addAttr(AType.LOOP, loop);
                    block.addAttr(AType.LOOP, loop);
                }
            });
        });
    }

    private static void registerLoops(MethodNode mth) {
        mth.getBasicBlocks().forEach(block -> {
            if (block.contains(AFlag.LOOP_START)) {
                block.getAll(AType.LOOP).forEach(mth::registerLoop);
            }
        });
    }

    private static void processNestedLoops(MethodNode mth) {
        if (mth.getLoopsCount() == 0) {
            return;
        }
        for (LoopInfo outLoop : mth.getLoops()) {
            for (LoopInfo innerLoop : mth.getLoops()) {
                if (outLoop == innerLoop) {
                    continue;
                }
                if (outLoop.getLoopBlocks().containsAll(innerLoop.getLoopBlocks())) {
                    LoopInfo parentLoop = innerLoop.getParentLoop();
                    if (parentLoop != null) {
                        if (parentLoop.getLoopBlocks().containsAll(outLoop.getLoopBlocks())) {
                            outLoop.setParentLoop(parentLoop);
                            innerLoop.setParentLoop(outLoop);
                        } else {
                            parentLoop.setParentLoop(outLoop);
                        }
                    } else {
                        innerLoop.setParentLoop(outLoop);
                    }
                }
            }
        }
    }

    private static boolean modifyBlocksTree(MethodNode mth) {
        List<BlockNode> basicBlocks = mth.getBasicBlocks();
        for (BlockNode block : basicBlocks) {
            if (block.getPredecessors().isEmpty() && block != mth.getEnterBlock()) {
                throw new JadxRuntimeException("Unreachable block: " + block);
            }
        }
        if (mergeExceptionHandlers(mth)) {
            removeMarkedBlocks(mth);
            return true;
        }
        for (BlockNode block : basicBlocks) {
            if (checkLoops(mth, block)) {
                return true;
            }
        }
        return splitReturn(mth);
    }

    private static boolean independentBlockTreeMod(MethodNode mth) {
        List<BlockNode> basicBlocks = mth.getBasicBlocks();
        boolean changed = false;
        for (BlockNode basicBlock : basicBlocks) {
            if (deduplicateBlockInsns(basicBlock)) {
                changed = true;
            }
        }
        for (BlockNode basicBlock : basicBlocks) {
            if (removeEmptyBlock(basicBlock)) {
                changed = true;
            }
        }
        if (BlockSplitter.removeEmptyDetachedBlocks(mth)) {
            changed = true;
        }
        return changed;
    }

    private static boolean checkLoops(MethodNode mth, BlockNode block) {
        List<LoopInfo> loops = block.getAll(AType.LOOP);
        int loopsCount = loops.size();
        if (loopsCount == 0) {
            return false;
        }
        if (loopsCount > 1 && splitLoops(mth, block, loops)) {
            return true;
        }
        if (loopsCount == 1) {
            LoopInfo loop = loops.get(0);
            return insertBlocksForBreak(mth, loop) || insertBlocksForContinue(mth, loop) || insertBlockForProdecessors(mth, loop);
        }
        return false;
    }

    private static boolean insertBlocksForBreak(MethodNode mth, LoopInfo loop) {
        boolean change = false;
        List<Edge> edges = loop.getExitEdges();
        if (!edges.isEmpty()) {
            for (Edge edge : edges) {
                BlockNode target = edge.getTarget();
                BlockNode source = edge.getSource();
                if (!target.contains(AFlag.SYNTHETIC) && !source.contains(AFlag.SYNTHETIC)) {
                    BlockSplitter.insertBlockBetween(mth, source, target);
                    change = true;
                }
            }
        }
        return change;
    }

    private static boolean insertBlocksForContinue(MethodNode mth, LoopInfo loop) {
        BlockNode loopEnd = loop.getEnd();
        boolean change = false;
        List<BlockNode> preds = loopEnd.getPredecessors();
        if (preds.size() > 1) {
            for (BlockNode pred : new ArrayList<>(preds)) {
                if (!pred.contains(AFlag.SYNTHETIC)) {
                    BlockSplitter.insertBlockBetween(mth, pred, loopEnd);
                    change = true;
                }
            }
        }
        return change;
    }

    private static boolean insertBlockForProdecessors(MethodNode mth, LoopInfo loop) {
        BlockNode loopHeader = loop.getStart();
        List<BlockNode> preds = loopHeader.getPredecessors();
        if (preds.size() > 2) {
            List<BlockNode> blocks = new LinkedList<>(preds);
            blocks.removeIf(block -> block.contains(AFlag.LOOP_END));
            BlockNode first = blocks.remove(0);
            BlockNode preHeader = BlockSplitter.insertBlockBetween(mth, first, loopHeader);
            blocks.forEach(block -> BlockSplitter.replaceConnection(block, loopHeader, preHeader));
            return true;
        }
        return false;
    }

    private static boolean splitLoops(MethodNode mth, BlockNode block, List<LoopInfo> loops) {
        boolean oneHeader = true;
        for (LoopInfo loop : loops) {
            if (loop.getStart() != block) {
                oneHeader = false;
                break;
            }
        }
        if (oneHeader) {
            BlockNode newLoopEnd = BlockSplitter.startNewBlock(mth, block.getStartOffset());
            newLoopEnd.add(AFlag.SYNTHETIC);
            connect(newLoopEnd, block);
            for (LoopInfo la : loops) {
                BlockSplitter.replaceConnection(la.getEnd(), block, newLoopEnd);
            }
            return true;
        }
        return false;
    }

    private static boolean mergeExceptionHandlers(MethodNode mth) {
        for (BlockNode block : mth.getBasicBlocks()) {
            ExcHandlerAttr excHandlerAttr = block.get(AType.EXC_HANDLER);
            if (excHandlerAttr != null) {
                List<BlockNode> blocksForMerge = collectExcHandlerBlocks(block, excHandlerAttr);
                if (mergeHandlers(mth, blocksForMerge, excHandlerAttr)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<BlockNode> collectExcHandlerBlocks(BlockNode block, ExcHandlerAttr excHandlerAttr) {
        List<BlockNode> successors = block.getSuccessors();
        if (successors.size() != 1) {
            return Collections.emptyList();
        }
        RegisterArg reg = getMoveExceptionRegister(block);
        if (reg == null) {
            return Collections.emptyList();
        }
        TryCatchBlock tryBlock = excHandlerAttr.getTryBlock();
        List<BlockNode> blocksForMerge = new ArrayList<>();
        BlockNode nextBlock = successors.get(0);
        for (BlockNode predBlock : nextBlock.getPredecessors()) {
            if (predBlock != block && checkOtherExcHandler(predBlock, tryBlock, reg)) {
                blocksForMerge.add(predBlock);
            }
        }
        return blocksForMerge;
    }

    private static boolean checkOtherExcHandler(BlockNode predBlock, TryCatchBlock tryBlock, RegisterArg reg) {
        ExcHandlerAttr otherExcHandlerAttr = predBlock.get(AType.EXC_HANDLER);
        if (otherExcHandlerAttr == null) {
            return false;
        }
        TryCatchBlock otherTryBlock = otherExcHandlerAttr.getTryBlock();
        if (tryBlock != otherTryBlock) {
            return false;
        }
        RegisterArg otherReg = getMoveExceptionRegister(predBlock);
        if (otherReg == null || reg.getRegNum() != otherReg.getRegNum()) {
            return false;
        }
        return true;
    }

    private static RegisterArg getMoveExceptionRegister(BlockNode block) {
        if (block.getInstructions().isEmpty()) {
            return null;
        }
        InsnNode insn = block.getInstructions().get(0);
        if (insn.getType() != InsnType.MOVE_EXCEPTION) {
            return null;
        }
        return insn.getResult();
    }

    private static boolean mergeHandlers(MethodNode mth, List<BlockNode> blocksForMerge, ExcHandlerAttr excHandlerAttr) {
        if (blocksForMerge.isEmpty()) {
            return false;
        }
        TryCatchBlock tryBlock = excHandlerAttr.getTryBlock();
        for (BlockNode block : blocksForMerge) {
            ExcHandlerAttr otherExcHandlerAttr = block.get(AType.EXC_HANDLER);
            ExceptionHandler excHandler = otherExcHandlerAttr.getHandler();
            excHandlerAttr.getHandler().addCatchTypes(excHandler.getCatchTypes());
            tryBlock.removeHandler(mth, excHandler);
            detachBlock(block);
        }
        return true;
    }

    private static boolean splitReturn(MethodNode mth) {
        if (mth.getExitBlocks().size() != 1) {
            return false;
        }
        BlockNode exitBlock = mth.getExitBlocks().get(0);
        if (exitBlock.getInstructions().size() != 1 || exitBlock.contains(AFlag.SYNTHETIC) || exitBlock.contains(AType.SPLITTER_BLOCK)) {
            return false;
        }
        List<BlockNode> preds = exitBlock.getPredecessors();
        if (preds.size() < 2) {
            return false;
        }
        preds = BlockUtils.filterPredecessors(exitBlock);
        if (preds.size() < 2) {
            return false;
        }
        InsnNode returnInsn = exitBlock.getInstructions().get(0);
        if (returnInsn.getArgsCount() != 0 && !isReturnArgAssignInPred(preds, returnInsn)) {
            return false;
        }
        boolean first = true;
        for (BlockNode pred : preds) {
            BlockNode newRetBlock = BlockSplitter.startNewBlock(mth, -1);
            newRetBlock.add(AFlag.SYNTHETIC);
            InsnNode newRetInsn;
            if (first) {
                newRetInsn = returnInsn;
                newRetBlock.add(AFlag.ORIG_RETURN);
                first = false;
            } else {
                newRetInsn = duplicateReturnInsn(returnInsn);
            }
            newRetBlock.getInstructions().add(newRetInsn);
            BlockSplitter.replaceConnection(pred, exitBlock, newRetBlock);
        }
        cleanExitNodes(mth);
        return true;
    }

    private static boolean isReturnArgAssignInPred(List<BlockNode> preds, InsnNode returnInsn) {
        RegisterArg arg = (RegisterArg) returnInsn.getArg(0);
        int regNum = arg.getRegNum();
        for (BlockNode pred : preds) {
            for (InsnNode insnNode : pred.getInstructions()) {
                RegisterArg result = insnNode.getResult();
                if (result != null && result.getRegNum() == regNum) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void cleanExitNodes(MethodNode mth) {
        Iterator<BlockNode> iterator = mth.getExitBlocks().iterator();
        while (iterator.hasNext()) {
            BlockNode exitBlock = iterator.next();
            if (exitBlock.getPredecessors().isEmpty()) {
                mth.getBasicBlocks().remove(exitBlock);
                iterator.remove();
            }
        }
    }

    private static InsnNode duplicateReturnInsn(InsnNode returnInsn) {
        InsnNode insn = new InsnNode(returnInsn.getType(), returnInsn.getArgsCount());
        if (returnInsn.getArgsCount() == 1) {
            RegisterArg arg = (RegisterArg) returnInsn.getArg(0);
            insn.addArg(InsnArg.reg(arg.getRegNum(), arg.getInitType()));
        }
        insn.copyAttributesFrom(returnInsn);
        insn.setOffset(returnInsn.getOffset());
        insn.setSourceLine(returnInsn.getSourceLine());
        return insn;
    }

    private static void removeMarkedBlocks(MethodNode mth) {
        mth.getBasicBlocks().removeIf(block -> {
            if (block.contains(AFlag.REMOVE)) {
                if (!block.getPredecessors().isEmpty() || !block.getSuccessors().isEmpty()) {
                    LOG.warn("Block {} not deleted, method: {}", block, mth);
                } else {
                    CatchAttr catchAttr = block.get(AType.CATCH_BLOCK);
                    if (catchAttr != null) {
                        catchAttr.getTryBlock().removeBlock(mth, block);
                    }
                    return true;
                }
            }
            return false;
        });
    }

    private static void detachBlock(BlockNode block) {
        for (BlockNode pred : block.getPredecessors()) {
            pred.getSuccessors().remove(block);
            pred.updateCleanSuccessors();
        }
        for (BlockNode successor : block.getSuccessors()) {
            successor.getPredecessors().remove(block);
        }
        block.add(AFlag.REMOVE);
        block.getInstructions().clear();
        block.getPredecessors().clear();
        block.getSuccessors().clear();
    }

    private static void clearBlocksState(MethodNode mth) {
        mth.getBasicBlocks().forEach(block -> {
            block.remove(AType.LOOP);
            block.remove(AFlag.LOOP_START);
            block.remove(AFlag.LOOP_END);
            block.setDoms(null);
            block.setIDom(null);
            block.setDomFrontier(null);
            block.getDominatesOn().clear();
        });
    }
}