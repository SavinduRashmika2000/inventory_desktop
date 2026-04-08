package com.sync.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncResponse {
    private int insertedCount;
    private int updatedCount;
    private List<String> errors;
}
