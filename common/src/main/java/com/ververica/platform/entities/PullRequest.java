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
@TypeInfo(PullRequest.CommitTypeInfoFactory.class)
public class PullRequest {
  private int number;
  private String state;
  private String title;
  private String description;
  private String creator;
  private String creatorEmail;
  private String[] labels;
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
  private LocalDateTime closedAt;
  private LocalDateTime mergedAt;
  private String mergeCommit;
  private int commentsCount;

  public static class CommitTypeInfoFactory extends TypeInfoFactory<PullRequest> {
    @Override
    public TypeInformation<PullRequest> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields =
          new HashMap<String, TypeInformation<?>>() {
            {
              put("number", Types.INT);
              put("state", Types.STRING);
              put("title", Types.STRING);
              put("description", Types.STRING);
              put("creator", Types.STRING);
              put("creatorEmail", Types.STRING);
              put("labels", Types.OBJECT_ARRAY(Types.STRING));
              put("createdAt", Types.LOCAL_DATE_TIME);
              put("updatedAt", Types.LOCAL_DATE_TIME);
              put("closedAt", Types.LOCAL_DATE_TIME);
              put("mergeCommit", Types.STRING);
              put("mergedAt", Types.LOCAL_DATE_TIME);
              put("commentsCount", Types.INT);
            }
          };
      return Types.POJO(PullRequest.class, fields);
    }
  }
}
