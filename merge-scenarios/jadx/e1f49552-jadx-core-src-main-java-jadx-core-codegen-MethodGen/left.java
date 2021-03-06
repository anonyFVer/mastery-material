package jadx.core.codegen;

import java.util.Iterator;
import java.util.List;
import com.android.dx.rop.code.AccessFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jadx.core.Consts;
import jadx.core.dex.attributes.AFlag;
import jadx.core.dex.attributes.AType;
import jadx.core.dex.attributes.annotations.MethodParameters;
import jadx.core.dex.attributes.nodes.JumpInfo;
import jadx.core.dex.info.AccessInfo;
import jadx.core.dex.info.ClassInfo;
import jadx.core.dex.instructions.IfNode;
import jadx.core.dex.instructions.InsnType;
import jadx.core.dex.instructions.args.ArgType;
import jadx.core.dex.instructions.args.CodeVar;
import jadx.core.dex.instructions.args.RegisterArg;
import jadx.core.dex.instructions.args.SSAVar;
import jadx.core.dex.nodes.InsnNode;
import jadx.core.dex.nodes.MethodNode;
import jadx.core.dex.trycatch.CatchAttr;
import jadx.core.dex.visitors.DepthTraversal;
import jadx.core.dex.visitors.FallbackModeVisitor;
import jadx.core.utils.InsnUtils;
import jadx.core.utils.Utils;
import jadx.core.utils.exceptions.CodegenException;
import jadx.core.utils.exceptions.DecodeException;

public class MethodGen {

    private static final Logger LOG = LoggerFactory.getLogger(MethodGen.class);

    private final MethodNode mth;

    private final ClassGen classGen;

    private final AnnotationGen annotationGen;

    private final NameGen nameGen;

    public MethodGen(ClassGen classGen, MethodNode mth) {
        this.mth = mth;
        this.classGen = classGen;
        this.annotationGen = classGen.getAnnotationGen();
        this.nameGen = new NameGen(mth, classGen.isFallbackMode());
    }

    public ClassGen getClassGen() {
        return classGen;
    }

    public NameGen getNameGen() {
        return nameGen;
    }

    public MethodNode getMethodNode() {
        return mth;
    }

    public boolean addDefinition(CodeWriter code) {
        if (mth.getMethodInfo().isClassInit()) {
            code.attachDefinition(mth);
            code.startLine("static");
            return true;
        }
        if (mth.contains(AFlag.ANONYMOUS_CONSTRUCTOR)) {
            code.startLine();
            code.attachDefinition(mth);
            return false;
        }
        annotationGen.addForMethod(code, mth);
        AccessInfo clsAccFlags = mth.getParentClass().getAccessFlags();
        AccessInfo ai = mth.getAccessFlags();
        if (clsAccFlags.isInterface()) {
            ai = ai.remove(AccessFlags.ACC_ABSTRACT);
            ai = ai.remove(AccessFlags.ACC_PUBLIC);
        }
        if (clsAccFlags.isAnnotation()) {
            ai = ai.remove(AccessFlags.ACC_PUBLIC);
        }
        if (mth.getMethodInfo().isRenamed() && !ai.isConstructor()) {
            code.startLine("/* renamed from: ").add(mth.getName()).add(" */");
        }
        code.startLineWithNum(mth.getSourceLine());
        code.add(ai.makeString());
        if (Consts.DEBUG) {
            code.add(mth.isVirtual() ? "/* virtual */ " : "/* direct */ ");
        }
        if (classGen.addGenericMap(code, mth.getGenericMap())) {
            code.add(' ');
        }
        if (ai.isConstructor()) {
            code.attachDefinition(mth);
            code.add(classGen.getClassNode().getShortName());
        } else {
            classGen.useType(code, mth.getReturnType());
            code.add(' ');
            code.attachDefinition(mth);
            code.add(mth.getAlias());
        }
        code.add('(');
        List<RegisterArg> args = mth.getArguments(false);
        if (mth.getMethodInfo().isConstructor() && mth.getParentClass().contains(AType.ENUM_CLASS)) {
            if (args.size() == 2) {
                args.clear();
            } else if (args.size() > 2) {
                args = args.subList(2, args.size());
            } else {
                mth.addComment("JADX WARN: Incorrect number of args for enum constructor: " + args.size() + " (expected >= 2)");
            }
        } else if (mth.contains(AFlag.SKIP_FIRST_ARG)) {
            args = args.subList(1, args.size());
        }
        addMethodArguments(code, args);
        code.add(')');
        annotationGen.addThrows(mth, code);
        return true;
    }

    private void addMethodArguments(CodeWriter code, List<RegisterArg> args) {
        MethodParameters paramsAnnotation = mth.get(AType.ANNOTATION_MTH_PARAMETERS);
        int i = 0;
        Iterator<RegisterArg> it = args.iterator();
        while (it.hasNext()) {
            RegisterArg mthArg = it.next();
            SSAVar ssaVar = mthArg.getSVar();
            CodeVar var;
            if (ssaVar == null) {
                var = CodeVar.fromMthArg(mthArg);
            } else {
                var = ssaVar.getCodeVar();
            }
            if (paramsAnnotation != null) {
                annotationGen.addForParameter(code, paramsAnnotation, i);
            }
            if (var.isFinal()) {
                code.add("final ");
            }
            ArgType argType = var.getType();
            if (!it.hasNext() && mth.getAccessFlags().isVarArgs()) {
                if (argType.isArray()) {
                    ArgType elType = argType.getArrayElement();
                    classGen.useType(code, elType);
                    code.add("...");
                } else {
                    mth.addComment("JADX INFO: Last argument in varargs method is not array: " + var);
                    classGen.useType(code, argType);
                }
            } else {
                classGen.useType(code, argType);
            }
            code.add(' ');
            code.add(nameGen.assignArg(var));
            i++;
            if (it.hasNext()) {
                code.add(", ");
            }
        }
    }

    public void addInstructions(CodeWriter code) throws CodegenException {
        if (mth.contains(AType.JADX_ERROR) || mth.contains(AFlag.INCONSISTENT_CODE) || mth.getRegion() == null) {
            code.startLine("/*");
            addFallbackMethodCode(code);
            code.startLine("*/");
            ClassInfo clsAlias = mth.getParentClass().getAlias();
            code.startLine("throw new UnsupportedOperationException(\"Method not decompiled: ").add(clsAlias.makeFullClsName(clsAlias.getShortName(), true)).add(".").add(mth.getAlias()).add("(").add(Utils.listToString(mth.getMethodInfo().getArgumentsTypes())).add("):").add(mth.getMethodInfo().getReturnType().toString()).add("\");");
        } else {
            RegionGen regionGen = new RegionGen(this);
            regionGen.makeRegion(code, mth.getRegion());
        }
    }

    public void addFallbackMethodCode(CodeWriter code) {
        if (mth.getInstructions() == null) {
            try {
                mth.unload();
                mth.load();
                DepthTraversal.visit(new FallbackModeVisitor(), mth);
            } catch (DecodeException e) {
                LOG.error("Error reload instructions in fallback mode:", e);
                code.startLine("// Can't load method instructions: " + e.getMessage());
                return;
            }
        }
        InsnNode[] insnArr = mth.getInstructions();
        if (insnArr == null) {
            code.startLine("// Can't load method instructions.");
            return;
        }
        if (mth.getThisArg() != null) {
            code.startLine(nameGen.useArg(mth.getThisArg())).add(" = this;");
        }
        addFallbackInsns(code, mth, insnArr, true);
    }

    public static void addFallbackInsns(CodeWriter code, MethodNode mth, InsnNode[] insnArr, boolean addLabels) {
        InsnGen insnGen = new InsnGen(getFallbackMethodGen(mth), true);
        InsnNode prevInsn = null;
        for (InsnNode insn : insnArr) {
            if (insn == null || insn.getType() == InsnType.NOP) {
                continue;
            }
            if (addLabels && needLabel(insn, prevInsn)) {
                code.decIndent();
                code.startLine(getLabelName(insn.getOffset()) + ":");
                code.incIndent();
            }
            try {
                code.startLine();
                RegisterArg resArg = insn.getResult();
                if (resArg != null) {
                    ArgType varType = resArg.getInitType();
                    if (varType.isTypeKnown()) {
                        code.add(varType.toString()).add(' ');
                    }
                }
                insnGen.makeInsn(insn, code, InsnGen.Flags.INLINE);
                CatchAttr catchAttr = insn.get(AType.CATCH_BLOCK);
                if (catchAttr != null) {
                    code.add("     // " + catchAttr);
                }
            } catch (CodegenException e) {
                LOG.debug("Error generate fallback instruction: ", e.getCause());
                code.startLine("// error: " + insn);
            }
            prevInsn = insn;
        }
    }

    private static boolean needLabel(InsnNode insn, InsnNode prevInsn) {
        if (insn.contains(AType.EXC_HANDLER)) {
            return true;
        }
        if (insn.contains(AType.JUMP)) {
            if (prevInsn != null && prevInsn.getType() == InsnType.IF) {
                List<JumpInfo> jumps = insn.getAll(AType.JUMP);
                if (jumps.size() == 1) {
                    JumpInfo jump = jumps.get(0);
                    if (jump.getSrc() == prevInsn.getOffset() && jump.getDest() == insn.getOffset()) {
                        int target = ((IfNode) prevInsn).getTarget();
                        return insn.getOffset() == target;
                    }
                }
            }
            return true;
        }
        return false;
    }

    public static MethodGen getFallbackMethodGen(MethodNode mth) {
        ClassGen clsGen = new ClassGen(mth.getParentClass(), null, true, true, true);
        return new MethodGen(clsGen, mth);
    }

    public static String getLabelName(int offset) {
        return "L_" + InsnUtils.formatOffset(offset);
    }
}