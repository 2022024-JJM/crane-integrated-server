package com.hanwha.cis.adapter.out.persistence;

import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CraneHistoryRepository extends JpaRepository<CraneHistoryEntity, CraneHistoryId> {

    @Query("select h from CraneHistoryEntity h where h.id.craneId = :craneId and h.id.time <= :at "
            + "order by h.id.time desc")
    List<CraneHistoryEntity> findLatestAtOrBefore(@Param("craneId") String craneId, @Param("at") Instant at,
            Pageable pageable);

    @Query("select h from CraneHistoryEntity h where h.id.craneId = :craneId "
            + "and h.id.time > :fromExclusive and h.id.time <= :toInclusive order by h.id.time asc")
    List<CraneHistoryEntity> findInRange(@Param("craneId") String craneId,
            @Param("fromExclusive") Instant fromExclusive, @Param("toInclusive") Instant toInclusive,
            Pageable pageable);
}
