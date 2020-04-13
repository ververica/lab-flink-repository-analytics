package com.ververica.platform.entities;

import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Commit {
  private Date timestamp;
  private String author;
  private List<FileChanged> filesChanged;
}
