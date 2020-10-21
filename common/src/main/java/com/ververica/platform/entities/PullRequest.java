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
  private String creator;
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
  private LocalDateTime closedAt;
  private LocalDateTime mergedAt;
  private boolean isMerged;
  private String mergedBy;
  private int commentsCount;
  private int reviewCommentCount;
  private int commitCount;
  private FileChanged[] filesChanged;
  private int linesAdded;
  private int linesRemoved;

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
              put("creator", Types.STRING);
              put("createdAt", Types.LOCAL_DATE_TIME);
              put("updatedAt", Types.LOCAL_DATE_TIME);
              put("closedAt", Types.LOCAL_DATE_TIME);
              put("mergedAt", Types.LOCAL_DATE_TIME);
              put("isMerged", Types.BOOLEAN);
              put("mergedBy", Types.STRING);
              put("commentsCount", Types.INT);
              put("reviewCommentCount", Types.INT);
              put("commitCount", Types.INT);
              put("filesChanged", Types.OBJECT_ARRAY(Types.POJO(FileChanged.class)));
              put("linesAdded", Types.INT);
              put("linesRemoved", Types.INT);
            }
          };
      return Types.POJO(PullRequest.class, fields);
    }
  }
}
