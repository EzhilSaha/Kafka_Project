// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Employee.proto

package probuff;

public final class EmployeeOuterClass {
  private EmployeeOuterClass() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface EmployeeOrBuilder extends
      // @@protoc_insertion_point(interface_extends:Employee)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required string name = 1;</code>
     */
    boolean hasName();
    /**
     * <code>required string name = 1;</code>
     */
    java.lang.String getName();
    /**
     * <code>required string name = 1;</code>
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>required int32 id = 2;</code>
     */
    boolean hasId();
    /**
     * <code>required int32 id = 2;</code>
     */
    int getId();

    /**
     * <code>optional string Office = 3;</code>
     */
    boolean hasOffice();
    /**
     * <code>optional string Office = 3;</code>
     */
    java.lang.String getOffice();
    /**
     * <code>optional string Office = 3;</code>
     */
    com.google.protobuf.ByteString
        getOfficeBytes();
  }
  /**
   * Protobuf type {@code Employee}
   */
  public  static final class Employee extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:Employee)
      EmployeeOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use Employee.newBuilder() to construct.
    private Employee(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private Employee() {
      name_ = "";
      id_ = 0;
      Office_ = "";
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private Employee(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000001;
              name_ = bs;
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              id_ = input.readInt32();
              break;
            }
            case 26: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000004;
              Office_ = bs;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return probuff.EmployeeOuterClass.internal_static_Employee_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return probuff.EmployeeOuterClass.internal_static_Employee_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              probuff.EmployeeOuterClass.Employee.class, probuff.EmployeeOuterClass.Employee.Builder.class);
    }

    private int bitField0_;
    public static final int NAME_FIELD_NUMBER = 1;
    private volatile java.lang.Object name_;
    /**
     * <code>required string name = 1;</code>
     */
    public boolean hasName() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string name = 1;</code>
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          name_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string name = 1;</code>
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int ID_FIELD_NUMBER = 2;
    private int id_;
    /**
     * <code>required int32 id = 2;</code>
     */
    public boolean hasId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required int32 id = 2;</code>
     */
    public int getId() {
      return id_;
    }

    public static final int Office_FIELD_NUMBER = 3;
    private volatile java.lang.Object Office_;
    /**
     * <code>optional string Office = 3;</code>
     */
    public boolean hasOffice() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional string Office = 3;</code>
     */
    public java.lang.String getOffice() {
      java.lang.Object ref = Office_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          Office_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string Office = 3;</code>
     */
    public com.google.protobuf.ByteString
        getOfficeBytes() {
      java.lang.Object ref = Office_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        Office_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasName()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt32(2, id_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 3, Office_);
      }
      unknownFields.writeTo(output);
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, id_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, Office_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof probuff.EmployeeOuterClass.Employee)) {
        return super.equals(obj);
      }
      probuff.EmployeeOuterClass.Employee other = (probuff.EmployeeOuterClass.Employee) obj;

      boolean result = true;
      result = result && (hasName() == other.hasName());
      if (hasName()) {
        result = result && getName()
            .equals(other.getName());
      }
      result = result && (hasId() == other.hasId());
      if (hasId()) {
        result = result && (getId()
            == other.getId());
      }
      result = result && (hasOffice() == other.hasOffice());
      if (hasOffice()) {
        result = result && getOffice()
            .equals(other.getOffice());
      }
      result = result && unknownFields.equals(other.unknownFields);
      return result;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasName()) {
        hash = (37 * hash) + NAME_FIELD_NUMBER;
        hash = (53 * hash) + getName().hashCode();
      }
      if (hasId()) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + getId();
      }
      if (hasOffice()) {
        hash = (37 * hash) + Office_FIELD_NUMBER;
        hash = (53 * hash) + getOffice().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static probuff.EmployeeOuterClass.Employee parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static probuff.EmployeeOuterClass.Employee parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static probuff.EmployeeOuterClass.Employee parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static probuff.EmployeeOuterClass.Employee parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(probuff.EmployeeOuterClass.Employee prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code Employee}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:Employee)
        probuff.EmployeeOuterClass.EmployeeOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return probuff.EmployeeOuterClass.internal_static_Employee_descriptor;
      }

      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return probuff.EmployeeOuterClass.internal_static_Employee_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                probuff.EmployeeOuterClass.Employee.class, probuff.EmployeeOuterClass.Employee.Builder.class);
      }

      // Construct using server.proto.EmployeeOuterClass.Employee.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        name_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        Office_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return probuff.EmployeeOuterClass.internal_static_Employee_descriptor;
      }

      public probuff.EmployeeOuterClass.Employee getDefaultInstanceForType() {
        return probuff.EmployeeOuterClass.Employee.getDefaultInstance();
      }

      public probuff.EmployeeOuterClass.Employee build() {
        probuff.EmployeeOuterClass.Employee result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public probuff.EmployeeOuterClass.Employee buildPartial() {
        probuff.EmployeeOuterClass.Employee result = new probuff.EmployeeOuterClass.Employee(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.name_ = name_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.id_ = id_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.Office_ = Office_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof probuff.EmployeeOuterClass.Employee) {
          return mergeFrom((probuff.EmployeeOuterClass.Employee)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(probuff.EmployeeOuterClass.Employee other) {
        if (other == probuff.EmployeeOuterClass.Employee.getDefaultInstance()) return this;
        if (other.hasName()) {
          bitField0_ |= 0x00000001;
          name_ = other.name_;
          onChanged();
        }
        if (other.hasId()) {
          setId(other.getId());
        }
        if (other.hasOffice()) {
          bitField0_ |= 0x00000004;
          Office_ = other.Office_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        if (!hasName()) {
          return false;
        }
        if (!hasId()) {
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        probuff.EmployeeOuterClass.Employee parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (probuff.EmployeeOuterClass.Employee) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object name_ = "";
      /**
       * <code>required string name = 1;</code>
       */
      public boolean hasName() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string name = 1;</code>
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            name_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string name = 1;</code>
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string name = 1;</code>
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 1;</code>
       */
      public Builder clearName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 1;</code>
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        name_ = value;
        onChanged();
        return this;
      }

      private int id_ ;
      /**
       * <code>required int32 id = 2;</code>
       */
      public boolean hasId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required int32 id = 2;</code>
       */
      public int getId() {
        return id_;
      }
      /**
       * <code>required int32 id = 2;</code>
       */
      public Builder setId(int value) {
        bitField0_ |= 0x00000002;
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int32 id = 2;</code>
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        id_ = 0;
        onChanged();
        return this;
      }

      private java.lang.Object Office_ = "";
      /**
       * <code>optional string Office = 3;</code>
       */
      public boolean hasOffice() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional string Office = 3;</code>
       */
      public java.lang.String getOffice() {
        java.lang.Object ref = Office_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            Office_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string Office = 3;</code>
       */
      public com.google.protobuf.ByteString
          getOfficeBytes() {
        java.lang.Object ref = Office_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          Office_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string Office = 3;</code>
       */
      public Builder setOffice(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        Office_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string Office = 3;</code>
       */
      public Builder clearOffice() {
        bitField0_ = (bitField0_ & ~0x00000004);
        Office_ = getDefaultInstance().getOffice();
        onChanged();
        return this;
      }
      /**
       * <code>optional string Office = 3;</code>
       */
      public Builder setOfficeBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        Office_ = value;
        onChanged();
        return this;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:Employee)
    }

    // @@protoc_insertion_point(class_scope:Employee)
    private static final probuff.EmployeeOuterClass.Employee DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new probuff.EmployeeOuterClass.Employee();
    }

    public static probuff.EmployeeOuterClass.Employee getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<Employee>
        PARSER = new com.google.protobuf.AbstractParser<Employee>() {
      public Employee parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Employee(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<Employee> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Employee> getParserForType() {
      return PARSER;
    }

    public probuff.EmployeeOuterClass.Employee getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Employee_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Employee_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\rEmployee.proto\"3\n\007Employee\022\014\n\004name\030\001 \002(\t" +
      "\022\n\n\002id\030\002 \002(\005\022\016\n\006Office\030\003 \001(\tB\016\n\014server.p" +
      "roto"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_Employee_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_Employee_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Employee_descriptor,
        new java.lang.String[] { "Name", "Id", "Office", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}