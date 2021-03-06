package jadx.core.dex.nodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.android.dex.ClassData;
import com.android.dex.ClassData.Field;
import com.android.dex.ClassData.Method;
import com.android.dex.ClassDef;
import com.android.dex.Dex;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jadx.core.Consts;
import jadx.core.codegen.CodeWriter;
import jadx.core.dex.attributes.AFlag;
import jadx.core.dex.attributes.annotations.Annotation;
import jadx.core.dex.attributes.nodes.LineAttrNode;
import jadx.core.dex.attributes.nodes.SourceFileAttr;
import jadx.core.dex.info.AccessInfo;
import jadx.core.dex.info.AccessInfo.AFType;
import jadx.core.dex.info.ClassInfo;
import jadx.core.dex.info.FieldInfo;
import jadx.core.dex.info.MethodInfo;
import jadx.core.dex.instructions.args.ArgType;
import jadx.core.dex.instructions.args.LiteralArg;
import jadx.core.dex.nodes.parser.AnnotationsParser;
import jadx.core.dex.nodes.parser.FieldInitAttr;
import jadx.core.dex.nodes.parser.SignatureParser;
import jadx.core.dex.nodes.parser.StaticValuesParser;
import jadx.core.utils.exceptions.DecodeException;
import jadx.core.utils.exceptions.JadxRuntimeException;
import static jadx.core.dex.nodes.ProcessState.UNLOADED;

public class ClassNode extends LineAttrNode implements ILoadable, ICodeNode {

    private static final Logger LOG = LoggerFactory.getLogger(ClassNode.class);

    private final DexNode dex;

    private final ClassInfo clsInfo;

    private AccessInfo accessFlags;

    private ArgType superClass;

    private List<ArgType> interfaces;

    private Map<ArgType, List<ArgType>> genericMap;

    private final List<MethodNode> methods;

    private final List<FieldNode> fields;

    private List<ClassNode> innerClasses = new ArrayList<>();

    private CodeWriter code;

    private ClassNode parentClass;

    private ProcessState state = ProcessState.NOT_LOADED;

    private final Set<ClassNode> dependencies = new HashSet<>();

    private Map<MethodInfo, MethodNode> mthInfoMap = Collections.emptyMap();

    public ClassNode(DexNode dex, ClassDef cls) {
        this.dex = dex;
        this.clsInfo = ClassInfo.fromDex(dex, cls.getTypeIndex());
        try {
            if (cls.getSupertypeIndex() == DexNode.NO_INDEX) {
                this.superClass = null;
            } else {
                this.superClass = dex.getType(cls.getSupertypeIndex());
            }
            this.interfaces = new ArrayList<>(cls.getInterfaces().length);
            for (short interfaceIdx : cls.getInterfaces()) {
                this.interfaces.add(dex.getType(interfaceIdx));
            }
            if (cls.getClassDataOffset() != 0) {
                ClassData clsData = dex.readClassData(cls);
                int mthsCount = clsData.getDirectMethods().length + clsData.getVirtualMethods().length;
                int fieldsCount = clsData.getStaticFields().length + clsData.getInstanceFields().length;
                methods = new ArrayList<>(mthsCount);
                fields = new ArrayList<>(fieldsCount);
                for (Method mth : clsData.getDirectMethods()) {
                    methods.add(new MethodNode(this, mth, false));
                }
                for (Method mth : clsData.getVirtualMethods()) {
                    methods.add(new MethodNode(this, mth, true));
                }
                for (Field f : clsData.getStaticFields()) {
                    fields.add(new FieldNode(this, f));
                }
                loadStaticValues(cls, fields);
                for (Field f : clsData.getInstanceFields()) {
                    fields.add(new FieldNode(this, f));
                }
            } else {
                methods = Collections.emptyList();
                fields = Collections.emptyList();
            }
            loadAnnotations(cls);
            parseClassSignature();
            setFieldsTypesFromSignature();
            int sfIdx = cls.getSourceFileIndex();
            if (sfIdx != DexNode.NO_INDEX) {
                String fileName = dex.getString(sfIdx);
                addSourceFilenameAttr(fileName);
            }
            int accFlagsValue;
            Annotation a = getAnnotation(Consts.DALVIK_INNER_CLASS);
            if (a != null) {
                accFlagsValue = (Integer) a.getValues().get("accessFlags");
            } else {
                accFlagsValue = cls.getAccessFlags();
            }
            this.accessFlags = new AccessInfo(accFlagsValue, AFType.CLASS);
            markAnonymousClass(this);
            buildCache();
        } catch (Exception e) {
            throw new JadxRuntimeException("Error decode class: " + clsInfo, e);
        }
    }

    public ClassNode(DexNode dex, String name, int accessFlags) {
        this.dex = dex;
        this.clsInfo = ClassInfo.fromName(dex.root(), name);
        this.interfaces = new ArrayList<>();
        this.methods = new ArrayList<>();
        this.fields = new ArrayList<>();
        this.accessFlags = new AccessInfo(accessFlags, AFType.CLASS);
        this.parentClass = this;
        dex.addClassNode(this);
    }

    private void loadAnnotations(ClassDef cls) {
        int offset = cls.getAnnotationsOffset();
        if (offset != 0) {
            try {
                new AnnotationsParser(this).parse(offset);
            } catch (Exception e) {
                LOG.error("Error parsing annotations in {}", this, e);
            }
        }
    }

    private void loadStaticValues(ClassDef cls, List<FieldNode> staticFields) throws DecodeException {
        for (FieldNode f : staticFields) {
            if (f.getAccessFlags().isFinal()) {
                f.addAttr(FieldInitAttr.NULL_VALUE);
            }
        }
        int offset = cls.getStaticValuesOffset();
        if (offset == 0) {
            return;
        }
        Dex.Section section = dex.openSection(offset);
        StaticValuesParser parser = new StaticValuesParser(dex, section);
        parser.processFields(staticFields);
        root().getConstValues().processConstFields(this, staticFields);
    }

    private void parseClassSignature() {
        SignatureParser sp = SignatureParser.fromNode(this);
        if (sp == null) {
            return;
        }
        try {
            genericMap = sp.consumeGenericMap();
            superClass = sp.consumeType();
            for (int i = 0; i < interfaces.size(); i++) {
                ArgType type = sp.consumeType();
                if (type != null) {
                    interfaces.set(i, type);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            LOG.error("Class signature parse error: {}", this, e);
        }
    }

    private void setFieldsTypesFromSignature() {
        for (FieldNode field : fields) {
            try {
                SignatureParser sp = SignatureParser.fromNode(field);
                if (sp != null) {
                    ArgType gType = sp.consumeType();
                    if (gType != null) {
                        field.setType(gType);
                    }
                }
            } catch (Exception e) {
                LOG.error("Field signature parse error: {}.{}", this.getFullName(), field.getName(), e);
            }
        }
    }

    private void addSourceFilenameAttr(String fileName) {
        if (fileName == null) {
            return;
        }
        if (fileName.endsWith(".java")) {
            fileName = fileName.substring(0, fileName.length() - 5);
        }
        if (fileName.isEmpty() || fileName.equals("SourceFile") || fileName.equals("\"")) {
            return;
        }
        if (clsInfo != null) {
            String name = clsInfo.getShortName();
            if (fileName.equals(name)) {
                return;
            }
            if (fileName.contains("$") && fileName.endsWith('$' + name)) {
                return;
            }
            ClassInfo parentClass = clsInfo.getTopParentClass();
            if (parentClass != null && fileName.equals(parentClass.getShortName())) {
                return;
            }
        }
        this.addAttr(new SourceFileAttr(fileName));
    }

    @Override
    public void load() {
        for (MethodNode mth : getMethods()) {
            try {
                mth.load();
            } catch (Exception e) {
                mth.addError("Method load error", e);
            }
        }
        for (ClassNode innerCls : getInnerClasses()) {
            innerCls.load();
        }
    }

    @Override
    public void unload() {
        for (MethodNode mth : getMethods()) {
            mth.unload();
        }
        for (ClassNode innerCls : getInnerClasses()) {
            innerCls.unload();
        }
        setState(UNLOADED);
    }

    private void buildCache() {
        mthInfoMap = new HashMap<>(methods.size());
        for (MethodNode mth : methods) {
            mthInfoMap.put(mth.getMethodInfo(), mth);
        }
    }

    @Nullable
    public ArgType getSuperClass() {
        return superClass;
    }

    public List<ArgType> getInterfaces() {
        return interfaces;
    }

    public Map<ArgType, List<ArgType>> getGenericMap() {
        return genericMap;
    }

    public List<MethodNode> getMethods() {
        return methods;
    }

    public List<FieldNode> getFields() {
        return fields;
    }

    public FieldNode getConstField(Object obj) {
        return getConstField(obj, true);
    }

    @Nullable
    public FieldNode getConstField(Object obj, boolean searchGlobal) {
        return root().getConstValues().getConstField(this, obj, searchGlobal);
    }

    @Nullable
    public FieldNode getConstFieldByLiteralArg(LiteralArg arg) {
        return root().getConstValues().getConstFieldByLiteralArg(this, arg);
    }

    public FieldNode searchFieldById(int id) {
        return searchField(FieldInfo.fromDex(dex, id));
    }

    public FieldNode searchField(FieldInfo field) {
        for (FieldNode f : fields) {
            if (f.getFieldInfo().equals(field)) {
                return f;
            }
        }
        return null;
    }

    public FieldNode searchFieldByNameAndType(FieldInfo field) {
        for (FieldNode f : fields) {
            if (f.getFieldInfo().equalsNameAndType(field)) {
                return f;
            }
        }
        return null;
    }

    public FieldNode searchFieldByName(String name) {
        for (FieldNode f : fields) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        return null;
    }

    public MethodNode searchMethod(MethodInfo mth) {
        return mthInfoMap.get(mth);
    }

    public MethodNode searchMethodByName(String shortId) {
        for (MethodNode m : methods) {
            if (m.getMethodInfo().getShortId().equals(shortId)) {
                return m;
            }
        }
        return null;
    }

    public MethodNode searchMethodById(int id) {
        return searchMethodByName(MethodInfo.fromDex(dex, id).getShortId());
    }

    public ClassNode getParentClass() {
        if (parentClass == null) {
            if (clsInfo.isInner()) {
                ClassNode parent = dex().resolveClass(clsInfo.getParentClass());
                parentClass = parent == null ? this : parent;
            } else {
                parentClass = this;
            }
        }
        return parentClass;
    }

    public ClassNode getTopParentClass() {
        ClassNode parent = getParentClass();
        return parent == this ? this : parent.getTopParentClass();
    }

    public List<ClassNode> getInnerClasses() {
        return innerClasses;
    }

    public void addInnerClass(ClassNode cls) {
        innerClasses.add(cls);
        cls.parentClass = this;
    }

    public boolean isEnum() {
        return getAccessFlags().isEnum() && getSuperClass() != null && getSuperClass().getObject().equals(ArgType.ENUM.getObject());
    }

    public boolean isAnonymous() {
        return clsInfo.isInner() && clsInfo.getAlias().getShortName().startsWith(Consts.ANONYMOUS_CLASS_PREFIX) && getDefaultConstructor() != null;
    }

    public boolean isLambdaCls() {
        return accessFlags.isSynthetic() && accessFlags.isFinal() && clsInfo.getType().getObject().contains(".-$$Lambda$") && countStaticFields() == 0;
    }

    private int countStaticFields() {
        int c = 0;
        for (FieldNode field : fields) {
            if (field.getAccessFlags().isStatic()) {
                c++;
            }
        }
        return c;
    }

    private static void markAnonymousClass(ClassNode cls) {
        if (cls.isAnonymous() || cls.isLambdaCls()) {
            cls.add(AFlag.ANONYMOUS_CLASS);
            cls.add(AFlag.DONT_GENERATE);
        }
    }

    @Nullable
    public MethodNode getClassInitMth() {
        return searchMethodByName("<clinit>()V");
    }

    @Nullable
    public MethodNode getDefaultConstructor() {
        for (MethodNode mth : methods) {
            if (mth.isDefaultConstructor()) {
                return mth;
            }
        }
        return null;
    }

    @Override
    public AccessInfo getAccessFlags() {
        return accessFlags;
    }

    @Override
    public void setAccessFlags(AccessInfo accessFlags) {
        this.accessFlags = accessFlags;
    }

    @Override
    public DexNode dex() {
        return dex;
    }

    @Override
    public RootNode root() {
        return dex.root();
    }

    @Override
    public String typeName() {
        return "class";
    }

    public String getRawName() {
        return clsInfo.getRawName();
    }

    public ClassInfo getClassInfo() {
        return clsInfo;
    }

    public ClassInfo getAlias() {
        return clsInfo.getAlias();
    }

    public String getShortName() {
        return clsInfo.getAlias().getShortName();
    }

    public String getFullName() {
        return clsInfo.getAlias().getFullName();
    }

    public String getPackage() {
        return clsInfo.getAlias().getPackage();
    }

    public void setCode(CodeWriter code) {
        this.code = code;
    }

    public CodeWriter getCode() {
        return code;
    }

    public ProcessState getState() {
        return state;
    }

    public void setState(ProcessState state) {
        this.state = state;
    }

    public Set<ClassNode> getDependencies() {
        return dependencies;
    }

    @Override
    public int hashCode() {
        return clsInfo.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ClassNode) {
            ClassNode other = (ClassNode) o;
            return clsInfo.equals(other.clsInfo);
        }
        return false;
    }

    @Override
    public String toString() {
        return clsInfo.getFullName();
    }
}