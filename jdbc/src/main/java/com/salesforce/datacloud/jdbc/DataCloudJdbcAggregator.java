/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This file exists only to satisfy Maven Central requirements.
 * The {@code com.salesforce.datacloud:jdbc} package is a convenience package that ties together at least {@code com.salesforce.datacloud:jdbc-grpc} and {@code com.salesforce.datacloud:jdbc-core}
 */
@Retention(RetentionPolicy.SOURCE)
@Target({})
public @interface DataCloudJdbcAggregator {}
