/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.deployment;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbCompositeKey;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.db.impl.DbString;
import io.camunda.zeebe.db.impl.DbTenantAwareKey;
import io.camunda.zeebe.db.impl.DbTenantAwareKey.PlacementType;
import io.camunda.zeebe.engine.EngineConfiguration;
import io.camunda.zeebe.engine.state.mutable.MutableFormState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.impl.record.value.deployment.FormRecord;
import java.util.Optional;

public class DbFormState implements MutableFormState {

  private static final int DEFAULT_VERSION_VALUE = 0;

  private final DbString tenantIdKey;
  private final DbLong dbFormKey;
  private final DbTenantAwareKey<DbLong> tenantAwareFormKey;
  private final PersistedForm dbPersistedForm;
  private final ColumnFamily<DbTenantAwareKey<DbLong>, PersistedForm> formsByKey;
  private final DbString dbFormId;
  private final VersionManager versionManager;
  private final DbLong formVersion;
  private final DbCompositeKey<DbString, DbLong> idAndVersionKey;
  private final DbTenantAwareKey<DbCompositeKey<DbString, DbLong>> tenantAwareIdAndVersionKey;
  private final ColumnFamily<DbTenantAwareKey<DbCompositeKey<DbString, DbLong>>, PersistedForm>
      formByIdAndVersionColumnFamily;
  private final Cache<TenantIdAndFormId, PersistedForm> formsByTenantIdAndIdCache;

  // CF84: [tenant id | form id | deployment key] => form key
  private final DbLong dbDeploymentKey;
  private final DbCompositeKey<DbString, DbLong> formIdAndDeploymentKey;
  private final DbTenantAwareKey<DbCompositeKey<DbString, DbLong>>
      tenantAwareFormIdAndDeploymentKey;
  private final DbLong dbFormKeyResult;
  private final ColumnFamily<DbTenantAwareKey<DbCompositeKey<DbString, DbLong>>, DbLong>
      formKeyByFormIdAndDeploymentKey;

  // CF93: [tenant id | form id | version tag] => form key
  private final DbString dbVersionTag;
  private final DbCompositeKey<DbString, DbString> formIdAndVersionTag;
  private final DbTenantAwareKey<DbCompositeKey<DbString, DbString>> tenantAwareFormIdAndVersionTag;
  private final ColumnFamily<DbTenantAwareKey<DbCompositeKey<DbString, DbString>>, DbLong>
      formKeyByFormIdAndVersionTag;

  public DbFormState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb,
      final TransactionContext transactionContext,
      final EngineConfiguration config) {
    tenantIdKey = new DbString();
    dbFormKey = new DbLong();
    tenantAwareFormKey = new DbTenantAwareKey<>(tenantIdKey, dbFormKey, PlacementType.PREFIX);
    dbPersistedForm = new PersistedForm();
    formsByKey =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.FORMS, transactionContext, tenantAwareFormKey, dbPersistedForm);

    dbFormId = new DbString();
    formVersion = new DbLong();
    idAndVersionKey = new DbCompositeKey<>(dbFormId, formVersion);
    tenantAwareIdAndVersionKey =
        new DbTenantAwareKey<>(tenantIdKey, idAndVersionKey, PlacementType.PREFIX);
    formByIdAndVersionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.FORM_BY_ID_AND_VERSION,
            transactionContext,
            tenantAwareIdAndVersionKey,
            dbPersistedForm);

    versionManager =
        new VersionManager(
            DEFAULT_VERSION_VALUE, zeebeDb, ZbColumnFamilies.FORM_VERSION, transactionContext);

    formsByTenantIdAndIdCache =
        CacheBuilder.newBuilder().maximumSize(config.getFormCacheCapacity()).build();

    // CF84
    dbDeploymentKey = new DbLong();
    formIdAndDeploymentKey = new DbCompositeKey<>(dbFormId, dbDeploymentKey);
    tenantAwareFormIdAndDeploymentKey =
        new DbTenantAwareKey<>(tenantIdKey, formIdAndDeploymentKey, PlacementType.PREFIX);
    dbFormKeyResult = new DbLong();
    formKeyByFormIdAndDeploymentKey =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.FORM_KEY_BY_FORM_ID_AND_DEPLOYMENT_KEY,
            transactionContext,
            tenantAwareFormIdAndDeploymentKey,
            dbFormKeyResult);

    // CF93
    dbVersionTag = new DbString();
    formIdAndVersionTag = new DbCompositeKey<>(dbFormId, dbVersionTag);
    tenantAwareFormIdAndVersionTag =
        new DbTenantAwareKey<>(tenantIdKey, formIdAndVersionTag, PlacementType.PREFIX);
    formKeyByFormIdAndVersionTag =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.FORM_KEY_BY_FORM_ID_AND_VERSION_TAG,
            transactionContext,
            tenantAwareFormIdAndVersionTag,
            dbFormKeyResult);
  }

  @Override
  public void storeFormInFormColumnFamily(final FormRecord record) {
    tenantIdKey.wrapString(record.getTenantId());
    dbFormKey.wrapLong(record.getFormKey());
    dbPersistedForm.wrap(record);
    formsByKey.upsert(tenantAwareFormKey, dbPersistedForm);
    formsByTenantIdAndIdCache.put(
        new TenantIdAndFormId(record.getTenantId(), record.getFormId()), dbPersistedForm.copy());
  }

  @Override
  public void storeFormInFormByIdAndVersionColumnFamily(final FormRecord record) {
    tenantIdKey.wrapString(record.getTenantId());
    dbFormId.wrapString(record.getFormId());
    formVersion.wrapLong(record.getVersion());
    dbPersistedForm.wrap(record);
    formByIdAndVersionColumnFamily.upsert(tenantAwareIdAndVersionKey, dbPersistedForm);
  }

  @Override
  public void updateLatestVersion(final FormRecord record) {
    versionManager.addResourceVersion(
        record.getFormId(), record.getVersion(), record.getTenantId());
  }

  @Override
  public void deleteFormInFormsColumnFamily(final FormRecord record) {
    tenantIdKey.wrapString(record.getTenantId());
    dbFormKey.wrapLong(record.getFormKey());
    formsByKey.deleteExisting(tenantAwareFormKey);
    formsByTenantIdAndIdCache.invalidate(
        new TenantIdAndFormId(record.getTenantId(), record.getFormId()));

    // Delete from CF84 if deploymentKey is set
    dbFormId.wrapString(record.getFormId());
    if (record.getDeploymentKey() >= 0) {
      dbDeploymentKey.wrapLong(record.getDeploymentKey());
      formKeyByFormIdAndDeploymentKey.deleteIfExists(tenantAwareFormIdAndDeploymentKey);
    }

    // Delete from CF93 if versionTag is set
    if (record.getVersionTag() != null && !record.getVersionTag().isEmpty()) {
      dbVersionTag.wrapString(record.getVersionTag());
      formKeyByFormIdAndVersionTag.deleteIfExists(tenantAwareFormIdAndVersionTag);
    }
  }

  @Override
  public void deleteFormInFormByIdAndVersionColumnFamily(final FormRecord record) {
    tenantIdKey.wrapString(record.getTenantId());
    dbFormId.wrapString(record.getFormId());
    formVersion.wrapLong(record.getVersion());
    formByIdAndVersionColumnFamily.deleteExisting(tenantAwareIdAndVersionKey);
  }

  @Override
  public void deleteFormInFormVersionColumnFamily(final FormRecord record) {
    versionManager.deleteResourceVersion(
        record.getFormId(), record.getVersion(), record.getTenantId());
  }

  @Override
  public Optional<PersistedForm> findLatestFormById(final String formId, final String tenantId) {
    tenantIdKey.wrapString(tenantId);
    final Optional<PersistedForm> cachedForm = getFormFromCache(tenantId, formId);
    if (cachedForm.isPresent()) {
      return cachedForm;
    }

    final PersistedForm persistedForm = getPersistedFormById(formId, tenantId);
    if (persistedForm == null) {
      return Optional.empty();
    }
    formsByTenantIdAndIdCache.put(new TenantIdAndFormId(tenantId, formId), persistedForm);
    return Optional.of(persistedForm);
  }

  @Override
  public Optional<PersistedForm> findFormByKey(final long formKey, final String tenantId) {
    tenantIdKey.wrapString(tenantId);
    dbFormKey.wrapLong(formKey);
    return Optional.ofNullable(formsByKey.get(tenantAwareFormKey)).map(PersistedForm::copy);
  }

  @Override
  public Optional<PersistedForm> findFormByIdAndDeploymentKey(
      final String formId, final long deploymentKey, final String tenantId) {
    tenantIdKey.wrapString(tenantId);
    dbFormId.wrapString(formId);
    dbDeploymentKey.wrapLong(deploymentKey);

    final var storedKey = formKeyByFormIdAndDeploymentKey.get(tenantAwareFormIdAndDeploymentKey);
    if (storedKey == null) {
      return Optional.empty();
    }
    return findFormByKey(storedKey.getValue(), tenantId);
  }

  @Override
  public Optional<PersistedForm> findFormByIdAndVersionTag(
      final String formId, final String versionTag, final String tenantId) {
    if (versionTag == null || versionTag.isEmpty()) {
      return Optional.empty();
    }
    tenantIdKey.wrapString(tenantId);
    dbFormId.wrapString(formId);
    dbVersionTag.wrapString(versionTag);

    final var storedKey = formKeyByFormIdAndVersionTag.get(tenantAwareFormIdAndVersionTag);
    if (storedKey == null) {
      return Optional.empty();
    }
    return findFormByKey(storedKey.getValue(), tenantId);
  }

  @Override
  public void forEachForm(final FormIdentifier previousForm, final PersistedFormVisitor visitor) {
    if (previousForm != null) {
      tenantIdKey.wrapString(previousForm.tenantId());
      dbFormKey.wrapLong(previousForm.key());
      final boolean[] skippedFirst = {false};
      formsByKey.whileTrue(
          tenantAwareFormKey,
          (key, form) -> {
            if (!skippedFirst[0]) {
              skippedFirst[0] = true;
              return true;
            }
            return visitor.visit(form);
          });
    } else {
      formsByKey.whileTrue((key, form) -> visitor.visit(form));
    }
  }

  @Override
  public void setMissingDeploymentKey(
      final String tenantId, final long formKey, final long deploymentKey) {
    tenantIdKey.wrapString(tenantId);
    dbFormKey.wrapLong(formKey);
    final var form = formsByKey.get(tenantAwareFormKey);
    if (form == null) {
      return;
    }
    final var formId = bufferAsString(form.getFormId());
    final var formVersionValue = form.getVersion();
    form.setDeploymentKey(deploymentKey);
    formsByKey.update(tenantAwareFormKey, form);

    // also update in formByIdAndVersionColumnFamily
    dbFormId.wrapString(formId);
    formVersion.wrapLong(formVersionValue);
    final var formByVersion = formByIdAndVersionColumnFamily.get(tenantAwareIdAndVersionKey);
    if (formByVersion != null) {
      formByVersion.setDeploymentKey(deploymentKey);
      formByIdAndVersionColumnFamily.update(tenantAwareIdAndVersionKey, formByVersion);
    }

    // Store the deployment key lookup in CF84
    dbDeploymentKey.wrapLong(deploymentKey);
    dbFormKeyResult.wrapLong(formKey);
    formKeyByFormIdAndDeploymentKey.upsert(tenantAwareFormIdAndDeploymentKey, dbFormKeyResult);

    // Invalidate cache
    formsByTenantIdAndIdCache.invalidate(new TenantIdAndFormId(tenantId, formId));
  }

  @Override
  public void addDeploymentKeyMapping(
      final String tenantId, final String formId, final long formKey, final long deploymentKey) {
    tenantIdKey.wrapString(tenantId);
    dbFormId.wrapString(formId);
    dbDeploymentKey.wrapLong(deploymentKey);
    dbFormKeyResult.wrapLong(formKey);
    formKeyByFormIdAndDeploymentKey.upsert(tenantAwareFormIdAndDeploymentKey, dbFormKeyResult);
  }

  @Override
  public void storeFormKeyByFormIdAndVersionTag(final FormRecord record) {
    final var versionTag = record.getVersionTag();
    if (versionTag == null || versionTag.isEmpty()) {
      return;
    }
    tenantIdKey.wrapString(record.getTenantId());
    dbFormId.wrapString(record.getFormId());
    dbVersionTag.wrapString(versionTag);
    dbFormKeyResult.wrapLong(record.getFormKey());
    formKeyByFormIdAndVersionTag.upsert(tenantAwareFormIdAndVersionTag, dbFormKeyResult);
  }

  @Override
  public int getNextFormVersion(final String formId, final String tenantId) {
    return (int) versionManager.getHighestResourceVersion(formId, tenantId) + 1;
  }

  @Override
  public void clearCache() {
    formsByTenantIdAndIdCache.invalidateAll();
    versionManager.clear();
  }

  private PersistedForm getPersistedFormById(final String formId, final String tenantId) {
    dbFormId.wrapString(formId);
    final long latestVersion = versionManager.getLatestResourceVersion(formId, tenantId);
    formVersion.wrapLong(latestVersion);
    final PersistedForm persistedForm =
        formByIdAndVersionColumnFamily.get(tenantAwareIdAndVersionKey);
    if (persistedForm == null) {
      return null;
    }
    return persistedForm.copy();
  }

  private Optional<PersistedForm> getFormFromCache(final String tenantId, final String formId) {
    return Optional.ofNullable(
        formsByTenantIdAndIdCache.getIfPresent(new TenantIdAndFormId(tenantId, formId)));
  }

  private record TenantIdAndFormId(String tenantId, String formId) {}
}
