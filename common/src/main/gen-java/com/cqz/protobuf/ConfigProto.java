// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: proto/test.proto

package com.cqz.protobuf;

public final class ConfigProto {
  private ConfigProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface DogConfigOrBuilder extends
      // @@protoc_insertion_point(interface_extends:DogConfig)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>string name = 1;</code>
     * @return The name.
     */
    java.lang.String getName();
    /**
     * <code>string name = 1;</code>
     * @return The bytes for name.
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>int32 age = 2;</code>
     * @return The age.
     */
    int getAge();

    /**
     * <code>bool adopted = 3;</code>
     * @return The adopted.
     */
    boolean getAdopted();
  }
  /**
   * Protobuf type {@code DogConfig}
   */
  public static final class DogConfig extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:DogConfig)
      DogConfigOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use DogConfig.newBuilder() to construct.
    private DogConfig(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private DogConfig() {
      name_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new DogConfig();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.cqz.protobuf.ConfigProto.internal_static_DogConfig_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.cqz.protobuf.ConfigProto.internal_static_DogConfig_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.cqz.protobuf.ConfigProto.DogConfig.class, com.cqz.protobuf.ConfigProto.DogConfig.Builder.class);
    }

    public static final int NAME_FIELD_NUMBER = 1;
    @SuppressWarnings("serial")
    private volatile java.lang.Object name_ = "";
    /**
     * <code>string name = 1;</code>
     * @return The name.
     */
    @java.lang.Override
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        name_ = s;
        return s;
      }
    }
    /**
     * <code>string name = 1;</code>
     * @return The bytes for name.
     */
    @java.lang.Override
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

    public static final int AGE_FIELD_NUMBER = 2;
    private int age_ = 0;
    /**
     * <code>int32 age = 2;</code>
     * @return The age.
     */
    @java.lang.Override
    public int getAge() {
      return age_;
    }

    public static final int ADOPTED_FIELD_NUMBER = 3;
    private boolean adopted_ = false;
    /**
     * <code>bool adopted = 3;</code>
     * @return The adopted.
     */
    @java.lang.Override
    public boolean getAdopted() {
      return adopted_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
      }
      if (age_ != 0) {
        output.writeInt32(2, age_);
      }
      if (adopted_ != false) {
        output.writeBool(3, adopted_);
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
      }
      if (age_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, age_);
      }
      if (adopted_ != false) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(3, adopted_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.cqz.protobuf.ConfigProto.DogConfig)) {
        return super.equals(obj);
      }
      com.cqz.protobuf.ConfigProto.DogConfig other = (com.cqz.protobuf.ConfigProto.DogConfig) obj;

      if (!getName()
          .equals(other.getName())) return false;
      if (getAge()
          != other.getAge()) return false;
      if (getAdopted()
          != other.getAdopted()) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + NAME_FIELD_NUMBER;
      hash = (53 * hash) + getName().hashCode();
      hash = (37 * hash) + AGE_FIELD_NUMBER;
      hash = (53 * hash) + getAge();
      hash = (37 * hash) + ADOPTED_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getAdopted());
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.cqz.protobuf.ConfigProto.DogConfig parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.cqz.protobuf.ConfigProto.DogConfig prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
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
     * Protobuf type {@code DogConfig}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:DogConfig)
        com.cqz.protobuf.ConfigProto.DogConfigOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.cqz.protobuf.ConfigProto.internal_static_DogConfig_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.cqz.protobuf.ConfigProto.internal_static_DogConfig_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.cqz.protobuf.ConfigProto.DogConfig.class, com.cqz.protobuf.ConfigProto.DogConfig.Builder.class);
      }

      // Construct using com.cqz.protobuf.ConfigProto.DogConfig.newBuilder()
      private Builder() {

      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);

      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        name_ = "";
        age_ = 0;
        adopted_ = false;
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.cqz.protobuf.ConfigProto.internal_static_DogConfig_descriptor;
      }

      @java.lang.Override
      public com.cqz.protobuf.ConfigProto.DogConfig getDefaultInstanceForType() {
        return com.cqz.protobuf.ConfigProto.DogConfig.getDefaultInstance();
      }

      @java.lang.Override
      public com.cqz.protobuf.ConfigProto.DogConfig build() {
        com.cqz.protobuf.ConfigProto.DogConfig result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public com.cqz.protobuf.ConfigProto.DogConfig buildPartial() {
        com.cqz.protobuf.ConfigProto.DogConfig result = new com.cqz.protobuf.ConfigProto.DogConfig(this);
        if (bitField0_ != 0) { buildPartial0(result); }
        onBuilt();
        return result;
      }

      private void buildPartial0(com.cqz.protobuf.ConfigProto.DogConfig result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.name_ = name_;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.age_ = age_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.adopted_ = adopted_;
        }
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.cqz.protobuf.ConfigProto.DogConfig) {
          return mergeFrom((com.cqz.protobuf.ConfigProto.DogConfig)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.cqz.protobuf.ConfigProto.DogConfig other) {
        if (other == com.cqz.protobuf.ConfigProto.DogConfig.getDefaultInstance()) return this;
        if (!other.getName().isEmpty()) {
          name_ = other.name_;
          bitField0_ |= 0x00000001;
          onChanged();
        }
        if (other.getAge() != 0) {
          setAge(other.getAge());
        }
        if (other.getAdopted() != false) {
          setAdopted(other.getAdopted());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new java.lang.NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10: {
                name_ = input.readStringRequireUtf8();
                bitField0_ |= 0x00000001;
                break;
              } // case 10
              case 16: {
                age_ = input.readInt32();
                bitField0_ |= 0x00000002;
                break;
              } // case 16
              case 24: {
                adopted_ = input.readBool();
                bitField0_ |= 0x00000004;
                break;
              } // case 24
              default: {
                if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                  done = true; // was an endgroup tag
                }
                break;
              } // default:
            } // switch (tag)
          } // while (!done)
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }
      private int bitField0_;

      private java.lang.Object name_ = "";
      /**
       * <code>string name = 1;</code>
       * @return The name.
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          name_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string name = 1;</code>
       * @return The bytes for name.
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
       * <code>string name = 1;</code>
       * @param value The name to set.
       * @return This builder for chaining.
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) { throw new NullPointerException(); }
        name_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <code>string name = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearName() {
        name_ = getDefaultInstance().getName();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }
      /**
       * <code>string name = 1;</code>
       * @param value The bytes for name to set.
       * @return This builder for chaining.
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) { throw new NullPointerException(); }
        checkByteStringIsUtf8(value);
        name_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      private int age_ ;
      /**
       * <code>int32 age = 2;</code>
       * @return The age.
       */
      @java.lang.Override
      public int getAge() {
        return age_;
      }
      /**
       * <code>int32 age = 2;</code>
       * @param value The age to set.
       * @return This builder for chaining.
       */
      public Builder setAge(int value) {
        
        age_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       * <code>int32 age = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearAge() {
        bitField0_ = (bitField0_ & ~0x00000002);
        age_ = 0;
        onChanged();
        return this;
      }

      private boolean adopted_ ;
      /**
       * <code>bool adopted = 3;</code>
       * @return The adopted.
       */
      @java.lang.Override
      public boolean getAdopted() {
        return adopted_;
      }
      /**
       * <code>bool adopted = 3;</code>
       * @param value The adopted to set.
       * @return This builder for chaining.
       */
      public Builder setAdopted(boolean value) {
        
        adopted_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }
      /**
       * <code>bool adopted = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearAdopted() {
        bitField0_ = (bitField0_ & ~0x00000004);
        adopted_ = false;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:DogConfig)
    }

    // @@protoc_insertion_point(class_scope:DogConfig)
    private static final com.cqz.protobuf.ConfigProto.DogConfig DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.cqz.protobuf.ConfigProto.DogConfig();
    }

    public static com.cqz.protobuf.ConfigProto.DogConfig getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<DogConfig>
        PARSER = new com.google.protobuf.AbstractParser<DogConfig>() {
      @java.lang.Override
      public DogConfig parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        Builder builder = newBuilder();
        try {
          builder.mergeFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.setUnfinishedMessage(builder.buildPartial());
        } catch (com.google.protobuf.UninitializedMessageException e) {
          throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
        } catch (java.io.IOException e) {
          throw new com.google.protobuf.InvalidProtocolBufferException(e)
              .setUnfinishedMessage(builder.buildPartial());
        }
        return builder.buildPartial();
      }
    };

    public static com.google.protobuf.Parser<DogConfig> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<DogConfig> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public com.cqz.protobuf.ConfigProto.DogConfig getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_DogConfig_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_DogConfig_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\020proto/test.proto\"7\n\tDogConfig\022\014\n\004name\030" +
      "\001 \001(\t\022\013\n\003age\030\002 \001(\005\022\017\n\007adopted\030\003 \001(\010B\037\n\020c" +
      "om.cqz.protobufB\013ConfigProtob\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_DogConfig_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_DogConfig_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_DogConfig_descriptor,
        new java.lang.String[] { "Name", "Age", "Adopted", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
