package com.hanwha.cis.adapter.out.persistence;

import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CraneHistoryDiffRepository extends JpaRepository<CraneHistoryDiffEntity, CraneHistoryId> {

    @Query("select d from CraneHistoryDiffEntity d where d.id.craneId = :craneId "
            + "and d.id.time > :fromExclusive and d.id.time <= :toInclusive order by d.id.time asc")
    List<CraneHistoryDiffEntity> findInRange(@Param("craneId") String craneId,
            @Param("fromExclusive") Instant fromExclusive, @Param("toInclusive") Instant toInclusive,
            Pageable pageable);
}
