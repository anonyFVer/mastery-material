package jadx.core.deobf;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import jadx.core.dex.attributes.AFlag;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jadx.api.JadxArgs;
import jadx.core.dex.attributes.AType;
import jadx.core.dex.attributes.nodes.SourceFileAttr;
import jadx.core.dex.info.ClassInfo;
import jadx.core.dex.info.FieldInfo;
import jadx.core.dex.info.MethodInfo;
import jadx.core.dex.instructions.args.ArgType;
import jadx.core.dex.nodes.ClassNode;
import jadx.core.dex.nodes.DexNode;
import jadx.core.dex.nodes.FieldNode;
import jadx.core.dex.nodes.MethodNode;

public class Deobfuscator {

    private static final Logger LOG = LoggerFactory.getLogger(Deobfuscator.class);

    private static final boolean DEBUG = false;

    public static final String CLASS_NAME_SEPARATOR = ".";

    public static final String INNER_CLASS_SEPARATOR = "$";

    private final JadxArgs args;

    @NotNull
    private final List<DexNode> dexNodes;

    private final DeobfPresets deobfPresets;

    private final Map<ClassInfo, DeobfClsInfo> clsMap = new HashMap<>();

    private final Map<FieldInfo, String> fldMap = new HashMap<>();

    private final Map<MethodInfo, String> mthMap = new HashMap<>();

    private final Map<MethodInfo, OverridedMethodsNode> ovrdMap = new HashMap<>();

    private final List<OverridedMethodsNode> ovrd = new ArrayList<>();

    private final PackageNode rootPackage = new PackageNode("");

    private final Set<String> pkgSet = new TreeSet<>();

    private final Set<String> reservedClsNames = new HashSet<>();

    private final int maxLength;

    private final int minLength;

    private final boolean useSourceNameAsAlias;

    private int pkgIndex = 0;

    private int clsIndex = 0;

    private int fldIndex = 0;

    private int mthIndex = 0;

    public Deobfuscator(JadxArgs args, @NotNull List<DexNode> dexNodes, File deobfMapFile) {
        this.args = args;
        this.dexNodes = dexNodes;
        this.minLength = args.getDeobfuscationMinLength();
        this.maxLength = args.getDeobfuscationMaxLength();
        this.useSourceNameAsAlias = args.isUseSourceNameAsClassAlias();
        this.deobfPresets = new DeobfPresets(this, deobfMapFile);
    }

    public void execute() {
        if (!args.isDeobfuscationForceSave()) {
            deobfPresets.load();
            initIndexes();
        }
        process();
        deobfPresets.save(args.isDeobfuscationForceSave());
        clear();
    }

    private void initIndexes() {
        pkgIndex = pkgSet.size();
        clsIndex = deobfPresets.getClsPresetMap().size();
        fldIndex = deobfPresets.getFldPresetMap().size();
        mthIndex = deobfPresets.getMthPresetMap().size();
    }

    private void preProcess() {
        for (DexNode dexNode : dexNodes) {
            for (ClassNode cls : dexNode.getClasses()) {
                Collections.addAll(reservedClsNames, cls.getPackage().split("\\."));
            }
        }
        for (DexNode dexNode : dexNodes) {
            for (ClassNode cls : dexNode.getClasses()) {
                preProcessClass(cls);
            }
        }
    }

    private void process() {
        preProcess();
        if (DEBUG) {
            dumpAlias();
        }
        for (DexNode dexNode : dexNodes) {
            for (ClassNode cls : dexNode.getClasses()) {
                processClass(cls);
            }
        }
        postProcess();
    }

    private void postProcess() {
        int id = 1;
        for (OverridedMethodsNode o : ovrd) {
            boolean aliasFromPreset = false;
            String aliasToUse = null;
            for (MethodInfo mth : o.getMethods()) {
                if (mth.isAliasFromPreset()) {
                    aliasToUse = mth.getAlias();
                    aliasFromPreset = true;
                }
            }
            for (MethodInfo mth : o.getMethods()) {
                if (aliasToUse == null) {
                    if (mth.isRenamed() && !mth.isAliasFromPreset()) {
                        mth.setAlias(String.format("mo%d%s", id, prepareNamePart(mth.getName())));
                    }
                    aliasToUse = mth.getAlias();
                }
                mth.setAlias(aliasToUse);
                mth.setAliasFromPreset(aliasFromPreset);
            }
            id++;
        }
    }

    void clear() {
        deobfPresets.clear();
        clsMap.clear();
        fldMap.clear();
        mthMap.clear();
        ovrd.clear();
        ovrdMap.clear();
    }

    private void resolveOverriding(MethodNode mth) {
        Set<ClassNode> clsParents = new LinkedHashSet<>();
        collectClassHierarchy(mth.getParentClass(), clsParents);
        String mthSignature = mth.getMethodInfo().makeSignature(false);
        Set<MethodInfo> overrideSet = new LinkedHashSet<>();
        for (ClassNode classNode : clsParents) {
            MethodInfo methodInfo = getMthOverride(classNode.getMethods(), mthSignature);
            if (methodInfo != null) {
                overrideSet.add(methodInfo);
            }
        }
        if (overrideSet.isEmpty()) {
            return;
        }
        OverridedMethodsNode overrideNode = getOverrideMethodsNode(overrideSet);
        if (overrideNode == null) {
            overrideNode = new OverridedMethodsNode(overrideSet);
            ovrd.add(overrideNode);
        }
        for (MethodInfo overrideMth : overrideSet) {
            if (!ovrdMap.containsKey(overrideMth)) {
                ovrdMap.put(overrideMth, overrideNode);
                overrideNode.add(overrideMth);
            }
        }
    }

    private OverridedMethodsNode getOverrideMethodsNode(Set<MethodInfo> overrideSet) {
        for (MethodInfo overrideMth : overrideSet) {
            OverridedMethodsNode node = ovrdMap.get(overrideMth);
            if (node != null) {
                return node;
            }
        }
        return null;
    }

    private MethodInfo getMthOverride(List<MethodNode> methods, String mthSignature) {
        for (MethodNode m : methods) {
            MethodInfo mthInfo = m.getMethodInfo();
            if (mthInfo.getShortId().startsWith(mthSignature)) {
                return mthInfo;
            }
        }
        return null;
    }

    private void collectClassHierarchy(ClassNode cls, Set<ClassNode> collected) {
        boolean added = collected.add(cls);
        if (added) {
            ArgType superClass = cls.getSuperClass();
            if (superClass != null) {
                ClassNode superNode = cls.dex().resolveClass(superClass);
                if (superNode != null) {
                    collectClassHierarchy(superNode, collected);
                }
            }
            for (ArgType argType : cls.getInterfaces()) {
                ClassNode interfaceNode = cls.dex().resolveClass(argType);
                if (interfaceNode != null) {
                    collectClassHierarchy(interfaceNode, collected);
                }
            }
        }
    }

    private void processClass(ClassNode cls) {
        ClassInfo clsInfo = cls.getClassInfo();
        String fullName = getClassFullName(clsInfo);
        if (!fullName.equals(clsInfo.getFullName())) {
            clsInfo.rename(cls.dex().root(), fullName);
        }
        for (FieldNode field : cls.getFields()) {
            if (field.contains(AFlag.DONT_RENAME))
                continue;
            renameField(field);
        }
        for (MethodNode mth : cls.getMethods()) {
            renameMethod(mth);
        }
        for (ClassNode innerCls : cls.getInnerClasses()) {
            processClass(innerCls);
        }
    }

    public void renameField(FieldNode field) {
        FieldInfo fieldInfo = field.getFieldInfo();
        String alias = getFieldAlias(field);
        if (alias != null) {
            fieldInfo.setAlias(alias);
        }
    }

    public void forceRenameField(FieldNode field) {
        field.getFieldInfo().setAlias(makeFieldAlias(field));
    }

    public void renameMethod(MethodNode mth) {
        String alias = getMethodAlias(mth);
        if (alias != null) {
            mth.getMethodInfo().setAlias(alias);
        }
        if (mth.isVirtual()) {
            resolveOverriding(mth);
        }
    }

    public void forceRenameMethod(MethodNode mth) {
        mth.getMethodInfo().setAlias(makeMethodAlias(mth));
        if (mth.isVirtual()) {
            resolveOverriding(mth);
        }
    }

    public void addPackagePreset(String origPkgName, String pkgAlias) {
        PackageNode pkg = getPackageNode(origPkgName, true);
        pkg.setAlias(pkgAlias);
    }

    private PackageNode getPackageNode(String fullPkgName, boolean create) {
        if (fullPkgName.isEmpty() || fullPkgName.equals(CLASS_NAME_SEPARATOR)) {
            return rootPackage;
        }
        PackageNode result = rootPackage;
        PackageNode parentNode;
        do {
            String pkgName;
            int idx = fullPkgName.indexOf(CLASS_NAME_SEPARATOR);
            if (idx > -1) {
                pkgName = fullPkgName.substring(0, idx);
                fullPkgName = fullPkgName.substring(idx + 1);
            } else {
                pkgName = fullPkgName;
                fullPkgName = "";
            }
            parentNode = result;
            result = result.getInnerPackageByName(pkgName);
            if (result == null && create) {
                result = new PackageNode(pkgName);
                parentNode.addInnerPackage(result);
            }
        } while (!fullPkgName.isEmpty() && result != null);
        return result;
    }

    String getNameWithoutPackage(ClassInfo clsInfo) {
        String prefix;
        ClassInfo parentClsInfo = clsInfo.getParentClass();
        if (parentClsInfo != null) {
            DeobfClsInfo parentDeobfClsInfo = clsMap.get(parentClsInfo);
            if (parentDeobfClsInfo != null) {
                prefix = parentDeobfClsInfo.makeNameWithoutPkg();
            } else {
                prefix = getNameWithoutPackage(parentClsInfo);
            }
            prefix += INNER_CLASS_SEPARATOR;
        } else {
            prefix = "";
        }
        return prefix + clsInfo.getShortName();
    }

    private void preProcessClass(ClassNode cls) {
        ClassInfo classInfo = cls.getClassInfo();
        String pkgFullName = classInfo.getPackage();
        PackageNode pkg = getPackageNode(pkgFullName, true);
        doPkg(pkg, pkgFullName);
        String alias = deobfPresets.getForCls(classInfo);
        if (alias != null) {
            clsMap.put(classInfo, new DeobfClsInfo(this, cls, pkg, alias));
        } else {
            if (!clsMap.containsKey(classInfo)) {
                String clsShortName = classInfo.getShortName();
                if (shouldRename(clsShortName) || reservedClsNames.contains(clsShortName)) {
                    makeClsAlias(cls);
                }
            }
        }
        for (ClassNode innerCls : cls.getInnerClasses()) {
            preProcessClass(innerCls);
        }
    }

    public String getClsAlias(ClassNode cls) {
        DeobfClsInfo deobfClsInfo = clsMap.get(cls.getClassInfo());
        if (deobfClsInfo != null) {
            return deobfClsInfo.getAlias();
        }
        return makeClsAlias(cls);
    }

    private String makeClsAlias(ClassNode cls) {
        ClassInfo classInfo = cls.getClassInfo();
        String alias = null;
        if (this.useSourceNameAsAlias) {
            alias = getAliasFromSourceFile(cls);
        }
        if (alias == null) {
            String clsName = classInfo.getShortName();
            alias = String.format("C%04d%s", clsIndex++, prepareNamePart(clsName));
        }
        PackageNode pkg = getPackageNode(classInfo.getPackage(), true);
        clsMap.put(classInfo, new DeobfClsInfo(this, cls, pkg, alias));
        return alias;
    }

    @Nullable
    private String getAliasFromSourceFile(ClassNode cls) {
        SourceFileAttr sourceFileAttr = cls.get(AType.SOURCE_FILE);
        if (sourceFileAttr == null) {
            return null;
        }
        if (cls.getClassInfo().isInner()) {
            return null;
        }
        String name = sourceFileAttr.getFileName();
        if (name.endsWith(".java")) {
            name = name.substring(0, name.length() - ".java".length());
        } else if (name.endsWith(".kt")) {
            name = name.substring(0, name.length() - ".kt".length());
        }
        if (!NameMapper.isValidIdentifier(name) || NameMapper.isReserved(name)) {
            return null;
        }
        for (DeobfClsInfo deobfClsInfo : clsMap.values()) {
            if (deobfClsInfo.getAlias().equals(name)) {
                return null;
            }
        }
        ClassNode otherCls = cls.dex().root().searchClassByName(cls.getPackage() + '.' + name);
        if (otherCls != null) {
            return null;
        }
        cls.remove(AType.SOURCE_FILE);
        return name;
    }

    @Nullable
    private String getFieldAlias(FieldNode field) {
        FieldInfo fieldInfo = field.getFieldInfo();
        String alias = fldMap.get(fieldInfo);
        if (alias != null) {
            return alias;
        }
        alias = deobfPresets.getForFld(fieldInfo);
        if (alias != null) {
            fldMap.put(fieldInfo, alias);
            return alias;
        }
        if (shouldRename(field.getName())) {
            return makeFieldAlias(field);
        }
        return null;
    }

    @Nullable
    private String getMethodAlias(MethodNode mth) {
        MethodInfo methodInfo = mth.getMethodInfo();
        if (methodInfo.isClassInit() || methodInfo.isConstructor()) {
            return null;
        }
        String alias = mthMap.get(methodInfo);
        if (alias != null) {
            return alias;
        }
        alias = deobfPresets.getForMth(methodInfo);
        if (alias != null) {
            mthMap.put(methodInfo, alias);
            methodInfo.setAliasFromPreset(true);
            return alias;
        }
        if (shouldRename(mth.getName())) {
            return makeMethodAlias(mth);
        }
        return null;
    }

    public String makeFieldAlias(FieldNode field) {
        String alias = String.format("f%d%s", fldIndex++, prepareNamePart(field.getName()));
        fldMap.put(field.getFieldInfo(), alias);
        return alias;
    }

    public String makeMethodAlias(MethodNode mth) {
        String alias = String.format("m%d%s", mthIndex++, prepareNamePart(mth.getName()));
        mthMap.put(mth.getMethodInfo(), alias);
        return alias;
    }

    private void doPkg(PackageNode pkg, String fullName) {
        if (pkgSet.contains(fullName)) {
            return;
        }
        pkgSet.add(fullName);
        PackageNode parentPkg = pkg.getParentPackage();
        while (!parentPkg.getName().isEmpty()) {
            if (!parentPkg.hasAlias()) {
                doPkg(parentPkg, parentPkg.getFullName());
            }
            parentPkg = parentPkg.getParentPackage();
        }
        String pkgName = pkg.getName();
        if (!pkg.hasAlias() && shouldRename(pkgName)) {
            String pkgAlias = String.format("p%03d%s", pkgIndex++, prepareNamePart(pkgName));
            pkg.setAlias(pkgAlias);
        }
    }

    private boolean shouldRename(String s) {
        int len = s.length();
        return len < minLength || len > maxLength || !NameMapper.isValidIdentifier(s);
    }

    private String prepareNamePart(String name) {
        if (name.length() > maxLength) {
            return 'x' + Integer.toHexString(name.hashCode());
        }
        return NameMapper.removeInvalidCharsMiddle(name);
    }

    private void dumpClassAlias(ClassNode cls) {
        PackageNode pkg = getPackageNode(cls.getPackage(), false);
        if (pkg != null) {
            if (!cls.getFullName().equals(getClassFullName(cls))) {
                LOG.info("Alias name for class '{}' is '{}'", cls.getFullName(), getClassFullName(cls));
            }
        } else {
            LOG.error("Can't find package node for '{}'", cls.getPackage());
        }
    }

    private void dumpAlias() {
        for (DexNode dexNode : dexNodes) {
            for (ClassNode cls : dexNode.getClasses()) {
                dumpClassAlias(cls);
            }
        }
    }

    private String getPackageName(String packageName) {
        PackageNode pkg = getPackageNode(packageName, false);
        if (pkg != null) {
            return pkg.getFullAlias();
        }
        return packageName;
    }

    private String getClassName(ClassInfo clsInfo) {
        DeobfClsInfo deobfClsInfo = clsMap.get(clsInfo);
        if (deobfClsInfo != null) {
            return deobfClsInfo.makeNameWithoutPkg();
        }
        return getNameWithoutPackage(clsInfo);
    }

    private String getClassFullName(ClassNode cls) {
        return getClassFullName(cls.getClassInfo());
    }

    private String getClassFullName(ClassInfo clsInfo) {
        DeobfClsInfo deobfClsInfo = clsMap.get(clsInfo);
        if (deobfClsInfo != null) {
            return deobfClsInfo.getFullName();
        }
        return getPackageName(clsInfo.getPackage()) + CLASS_NAME_SEPARATOR + getClassName(clsInfo);
    }

    public Map<ClassInfo, DeobfClsInfo> getClsMap() {
        return clsMap;
    }

    public Map<FieldInfo, String> getFldMap() {
        return fldMap;
    }

    public Map<MethodInfo, String> getMthMap() {
        return mthMap;
    }

    public PackageNode getRootPackage() {
        return rootPackage;
    }
}