/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink.metadata;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;

import java.util.Map;
import java.util.Collection;
import java.util.stream.Collectors;

public class SinkRecordField {

  private final Schema schema;
  private final String name;
  private final boolean isPrimaryKey;

  public SinkRecordField(Schema schema, String name, boolean isPrimaryKey) {
    this.schema = schema;
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
  }

  public String schemaName() {
    return schema.name();
  }

  public Map<String, String> schemaParameters() {
    return schema.parameters();
  }

  public Schema.Type schemaType() {
    return schema.type();
  }

  public boolean isCompositeType() { return schemaType().equals(Schema.Type.STRUCT); }

  public String name() {
    return name;
  }

  public Collection<String> fieldNamesForCompositeType() {
    return schema.fields().stream().map(Field::name).collect(Collectors.toList());
  }

  public boolean isOptional() {
    return !isPrimaryKey && schema.isOptional();
  }

  public Object defaultValue() {
    return schema.defaultValue();
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  @Override
  public String toString() {
    return "SinkRecordField{"
           + "schema=" + schema
           + ", name='" + name + '\''
           + ", isPrimaryKey=" + isPrimaryKey
           + '}';
  }
}
