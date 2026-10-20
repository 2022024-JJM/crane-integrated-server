package com.hanwha.cis;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** 헥사고날 의존 방향: domain ← application ← adapter/config. 어댑터끼리는 서로 모른다. */
@AnalyzeClasses(packages = "com.hanwha.cis", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule domainDependsOnlyOnDomainAndJdk = classes()
            .that().resideInAPackage("..domain..")
            .should().onlyDependOnClassesThat().resideInAnyPackage("..domain..", "java..");

    @ArchTest
    static final ArchRule applicationDependsOnlyOnDomainAndApplication = classes()
            .that().resideInAPackage("..application..")
            .should().onlyDependOnClassesThat()
            .resideInAnyPackage("..domain..", "..application..", "java..", "org.slf4j..");

    @ArchTest
    static final ArchRule adaptersDoNotDependOnEachOther = slices()
            .matching("com.hanwha.cis.adapter.(**)")
            .should().notDependOnEachOther();

    @ArchTest
    static final ArchRule domainHasNoFrameworkDependencies = noClasses()
            .that().resideInAPackage("..domain..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("org.springframework..", "jakarta.persistence..", "com.fasterxml..", "tools.jackson..");

    @ArchTest
    static final ArchRule coreDoesNotDependOnConfigOrAdapters = noClasses()
            .that().resideInAnyPackage("..domain..", "..application..")
            .should().dependOnClassesThat().resideInAnyPackage("..config..", "..adapter..");
}
