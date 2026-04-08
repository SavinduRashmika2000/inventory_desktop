package com.sync.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncStatusResponse {
    private String tableName;
    private LocalDateTime lastSyncTimestamp;
    private Integer totalRecords;
}
