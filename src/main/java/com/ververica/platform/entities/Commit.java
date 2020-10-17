package com.ververica.platform.entities;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@TypeInfo(Commit.CommitTypeInfoFactory.class)
public class Commit {
  private LocalDateTime commitDate;
  private String committer;
  private String author;
  private LocalDateTime authorDate;
  private FileChanged[] filesChanged;

  public static class CommitTypeInfoFactory extends TypeInfoFactory<Commit> {
    @Override
    public TypeInformation<Commit> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields =
          new HashMap<String, TypeInformation<?>>() {
            {
              put("commitDate", Types.LOCAL_DATE_TIME);
              put("committer", Types.STRING);
              put("author", Types.STRING);
              put("authorDate", Types.LOCAL_DATE_TIME);
              put("filesChanged", Types.OBJECT_ARRAY(Types.POJO(FileChanged.class)));
            }
          };
      return Types.POJO(Commit.class, fields);
    }
  }
}
