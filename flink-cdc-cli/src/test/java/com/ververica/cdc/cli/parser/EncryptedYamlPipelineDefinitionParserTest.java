/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.cli.parser;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava30.com.google.common.io.Resources;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.shade.utils.ConfigShadeUtils;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.ververica.cdc.cli.encryptor.RSAConfigShade.SECRET_ENCRYPTOR_PRIVATE_KEY_STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link YamlPipelineDefinitionParser} with encrypted yaml pipeline definition. */
class EncryptedYamlPipelineDefinitionParserTest {

    private static final String USERNAME = "flinkcdc";

    private static final String PASSWORD = "flinkcdc_password";

    @Test
    public void testDecryptAndEncrypt() {
        String encryptUsername = "ZmxpbmtjZGM=";
        String encryptPassword = "ZmxpbmtjZGNfcGFzc3dvcmQ=";
        String decryptUsername = ConfigShadeUtils.decryptOption("base64", encryptUsername);
        String decryptPassword = ConfigShadeUtils.decryptOption("base64", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);
    }

    @Test
    void testDecryptDefinitionWithDefaultShade() throws Exception {
        URL resource =
                Resources.getResource(
                        "definitions/mysql-to-doris-encrypted-with-default-shade.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(decryptedDefWithDefaultShade);
    }

    @Test
    void testDecryptDefinitionWithBase64Shade() throws Exception {
        URL resource =
                Resources.getResource(
                        "definitions/mysql-to-doris-encrypted-with-base64-shade.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(decryptedDefWithBase64Encode);
    }

    @Test
    void testDecryptDefinitionWithDERKey() throws Exception {
        URL resource =
                Resources.getResource("definitions/mysql-to-doris-encrypted-with-DER-key.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(decryptedDefWithDERKey);
    }

    @Test
    void testDecryptDefinitionWithPEMKeyString() throws Exception {
        URL resource =
                Resources.getResource(
                        "definitions/mysql-to-doris-encrypted-with-PEM-keystring.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(decryptedDefWithPEMKeyString);
    }

    @Test
    void testUdalDecryptDefinitionWithDERKey() throws Exception {
        URL resource =
                Resources.getResource("definitions/udal-to-doris-encrypted-with-DER-key.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(udalDecryptedDefWithDERKey());
    }

    private final PipelineDef decryptedDefWithDefaultShade =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("hostname", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "password1")
                                            .put("tables", "replication.cluster")
                                            .put("server-id", "5400-5404")
                                            .put("server-time-zone", "Asia/Shanghai")
                                            .build())),
                    new SinkDef(
                            "doris",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("fenodes", "localhost:8035")
                                            .put("username", "root")
                                            .put("password", "password2")
                                            .put(
                                                    "table.create.properties.light_schema_change",
                                                    "true")
                                            .put("table.create.properties.replication_num", "1")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "replication.cluster",
                                    "test.cluster",
                                    "sync table to one destination table")),
                    null,
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "Sync MySQL Database to Doris")
                                    .put("parallelism", "2")
                                    .build()));

    private final PipelineDef decryptedDefWithBase64Encode =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("hostname", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "password1")
                                            .put("tables", "replication.cluster")
                                            .put("server-id", "5400-5404")
                                            .put("server-time-zone", "Asia/Shanghai")
                                            .build())),
                    new SinkDef(
                            "doris",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("fenodes", "localhost:8035")
                                            .put("username", "root")
                                            .put("password", "password2")
                                            .put(
                                                    "table.create.properties.light_schema_change",
                                                    "true")
                                            .put("table.create.properties.replication_num", "1")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "replication.cluster",
                                    "test.cluster",
                                    "sync table to one destination table")),
                    null,
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "Sync MySQL Database to Doris")
                                    .put("parallelism", "2")
                                    .put("shade.identifier", "base64")
                                    .put("shade.sensitive.keywords", "password;username")
                                    .build()));

    private final PipelineDef decryptedDefWithDERKey =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("hostname", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "password1")
                                            .put("tables", "replication.cluster")
                                            .put("server-id", "5400-5404")
                                            .put("server-time-zone", "Asia/Shanghai")
                                            .build())),
                    new SinkDef(
                            "doris",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("fenodes", "localhost:8035")
                                            .put("username", "root")
                                            .put("password", "password2")
                                            .put(
                                                    "table.create.properties.light_schema_change",
                                                    "true")
                                            .put("table.create.properties.replication_num", "1")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "replication.cluster",
                                    "test.cluster",
                                    "sync table to one destination table")),
                    null,
                    Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("name", "Sync MySQL Database to Doris")
                                            .put("parallelism", "2")
                                            .put("jasypt-encryptor.private-key-format", "DER")
                                            .put(
                                                    "jasypt-encryptor.private-key-location",
                                                    "src/test/resources/private_key.der")
                                            .put("shade.identifier", "rsa")
                                            .put("shade.sensitive.keywords", "password")
                                            .build())
                            .set(
                                    PipelineOptions.ADDITIONAL_FLINK_CONF,
                                    ImmutableMap.of(
                                            SECRET_ENCRYPTOR_PRIVATE_KEY_STRING.key(),
                                            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCVx0Qtu1PTzSGc7Uwg96NvgttYzNzjouaBeoQwzwBm0QpynitoehJMAyBLxqX4Z7eyqEUVem5oIkKYPxiNGVsEBOda0r/u1KxD5bmq0mdVF3xDKPY7j/ogrA//mg7wZPU6fMZPmzea585d4GPMC9N78L0I+N9Plq3BisPoltx5JfBkXdvbvS6AE72raNEO24TPvcPYgXaIuL2h2uF4oX6D8kmsnA1hGuz2gT1ZncIgCVdfM5FaNppsKLKM1CKOq9Xjhe2wKDThYnzDPvhx0NDfp/AnvEUUQB/ZKJYnqzZpEt25s8bokB5BBhZHGrpzOx1RvF5N0Olx2HOxmxPeNEfrAgMBAAECggEAXEE7xmtfC9OLA6zKOnguC+5vCvhMik35awE60RF/rtTNeaHhHN4rzPQT/XijHClOZdXtj3g07yFDIaGjjq1yeTxIAJJVNFd8r+mc/hHRIgFwQbHGUROyvlMdG5QZ5YpZ3ieiwr/ZdSmY3AvtjG4wWmQDG7anXc9ywQam+umJbXi2qrfikPkUteLK7/JLLVpOYSEdNxKvpi99ehOKvIvfOXZdzsemsDKso2M7M6m/50JThfgM4Y112H8D77SeGGCoS0JjqEH+Y9veFhIGNjMkEKo0+mCmdC3Zucc5SpNHWUJbsZ6NnMJK2vdDzMCvW2OAM16K6VQdrp1cNJFT8fZGuQKBgQDD/gH4C9J4QKjphdgO/Tc7MNfuscIZSvNxcv/KNeCcjWMiNNp98W1yLDkjqvZKsVLtT4xrvLJLyz/425GFGqwZdrnuPLJ4KDYAideYlaeYGHkrg3GgkvDdXw5XNkZzl2OgwS42miOI3jrLgDxETkWdm5GOlFFYqeLcWMHaA40LbQKBgQDDov1Rpz4gtty3UylhH7Rvf2HWmJpMTzj0lHh4ctBvKvbVs61O06kPeT2M1mrq6rZsCbT9+AMo8H5Ollkv1BxYlhplWOCS2Wn2kIbl15LbD1nOUm8NUT455hqbXqwQAHuNfJHFLRK6lDpQOW5HzEUFf9NrYP+ElmXWCu38X+5xtwKBgQCy7NNzVaehbLZH6o7isDyZ5u9a1CE04f35Vlk6i5Ewmssj0UieraeTEdhgPZV3fwcL6xFw0eWb2EPgtuLUtxLidEctvW/YzizutOiEPiuwaLdGCEvVohAOqNb4u2353P1xJqs/4dwK+YaHfdyHJ0XaqslpdvIElaOsMcNXeuJqyQKBgDm27dCW7CcuizKyH/T9K3fxNmXeozZ78KuG2Xt/M6p4HFWzKh+lJazg8Z2I7AXdtG9u9awu7I+5UynQBQOtKaegsKzRaX8pEK+it4k67aIehzns68IcA6WFErhfV5do7Qoyg7aNs4bDj+h26OfZhUs4CKEW/oqY6/YXR1o62tdFAoGBAK2AkTkGLWksxIpAMJ+GwPwxIlK6UOtoyxVefIKEy4bVt4vQZFLXwsKKzSTZX5z9CDzV/MUp51Gao9eo0TJkdtjUhNHGV4jdIUzB07V3BoL7nenxNk7Es7Hrp4Jljm8QgILtaeTIDd9pq7GciDi2WHsnD9Ipc1pNK8Rj0+8I8Pdx")));
    private final PipelineDef decryptedDefWithPEMKeyString =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("hostname", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "password1")
                                            .put("tables", "replication.cluster")
                                            .put("server-id", "5400-5404")
                                            .put("server-time-zone", "Asia/Shanghai")
                                            .build())),
                    new SinkDef(
                            "doris",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("fenodes", "localhost:8035")
                                            .put("username", "root")
                                            .put("password", "password2")
                                            .put(
                                                    "table.create.properties.light_schema_change",
                                                    "true")
                                            .put("table.create.properties.replication_num", "1")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "replication.cluster",
                                    "test.cluster",
                                    "sync table to one destination table")),
                    null,
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "Sync MySQL Database to Doris")
                                    .put("parallelism", "2")
                                    .put("jasypt-encryptor.private-key-format", "PEM")
                                    .put(
                                            "jasypt-encryptor.private-key-string",
                                            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCVx0Qtu1PTzSGc7Uwg96NvgttYzNzjouaBeoQwzwBm0QpynitoehJMAyBLxqX4Z7eyqEUVem5oIkKYPxiNGVsEBOda0r/u1KxD5bmq0mdVF3xDKPY7j/ogrA//mg7wZPU6fMZPmzea585d4GPMC9N78L0I+N9Plq3BisPoltx5JfBkXdvbvS6AE72raNEO24TPvcPYgXaIuL2h2uF4oX6D8kmsnA1hGuz2gT1ZncIgCVdfM5FaNppsKLKM1CKOq9Xjhe2wKDThYnzDPvhx0NDfp/AnvEUUQB/ZKJYnqzZpEt25s8bokB5BBhZHGrpzOx1RvF5N0Olx2HOxmxPeNEfrAgMBAAECggEAXEE7xmtfC9OLA6zKOnguC+5vCvhMik35awE60RF/rtTNeaHhHN4rzPQT/XijHClOZdXtj3g07yFDIaGjjq1yeTxIAJJVNFd8r+mc/hHRIgFwQbHGUROyvlMdG5QZ5YpZ3ieiwr/ZdSmY3AvtjG4wWmQDG7anXc9ywQam+umJbXi2qrfikPkUteLK7/JLLVpOYSEdNxKvpi99ehOKvIvfOXZdzsemsDKso2M7M6m/50JThfgM4Y112H8D77SeGGCoS0JjqEH+Y9veFhIGNjMkEKo0+mCmdC3Zucc5SpNHWUJbsZ6NnMJK2vdDzMCvW2OAM16K6VQdrp1cNJFT8fZGuQKBgQDD/gH4C9J4QKjphdgO/Tc7MNfuscIZSvNxcv/KNeCcjWMiNNp98W1yLDkjqvZKsVLtT4xrvLJLyz/425GFGqwZdrnuPLJ4KDYAideYlaeYGHkrg3GgkvDdXw5XNkZzl2OgwS42miOI3jrLgDxETkWdm5GOlFFYqeLcWMHaA40LbQKBgQDDov1Rpz4gtty3UylhH7Rvf2HWmJpMTzj0lHh4ctBvKvbVs61O06kPeT2M1mrq6rZsCbT9+AMo8H5Ollkv1BxYlhplWOCS2Wn2kIbl15LbD1nOUm8NUT455hqbXqwQAHuNfJHFLRK6lDpQOW5HzEUFf9NrYP+ElmXWCu38X+5xtwKBgQCy7NNzVaehbLZH6o7isDyZ5u9a1CE04f35Vlk6i5Ewmssj0UieraeTEdhgPZV3fwcL6xFw0eWb2EPgtuLUtxLidEctvW/YzizutOiEPiuwaLdGCEvVohAOqNb4u2353P1xJqs/4dwK+YaHfdyHJ0XaqslpdvIElaOsMcNXeuJqyQKBgDm27dCW7CcuizKyH/T9K3fxNmXeozZ78KuG2Xt/M6p4HFWzKh+lJazg8Z2I7AXdtG9u9awu7I+5UynQBQOtKaegsKzRaX8pEK+it4k67aIehzns68IcA6WFErhfV5do7Qoyg7aNs4bDj+h26OfZhUs4CKEW/oqY6/YXR1o62tdFAoGBAK2AkTkGLWksxIpAMJ+GwPwxIlK6UOtoyxVefIKEy4bVt4vQZFLXwsKKzSTZX5z9CDzV/MUp51Gao9eo0TJkdtjUhNHGV4jdIUzB07V3BoL7nenxNk7Es7Hrp4Jljm8QgILtaeTIDd9pq7GciDi2WHsnD9Ipc1pNK8Rj0+8I8Pdx")
                                    .put("shade.identifier", "rsa")
                                    .put("shade.sensitive.keywords", "password")
                                    .build()));

    private PipelineDef udalDecryptedDefWithDERKey() {
        List<SourceDef> sourceDefs = new ArrayList<>();
        SourceDef sourceDef1 =
                new SourceDef(
                        "mysql",
                        "source-database",
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("hostname", "localhost1")
                                        .put("port", "3001")
                                        .put("username", "admin")
                                        .put("password", "password1")
                                        .put("tables", "accum_[0-9]+.accumulation")
                                        .put("server-id", "5400-5404")
                                        .put("server-time-zone", "Asia/Shanghai")
                                        .build()));

        SourceDef sourceDef2 =
                new SourceDef(
                        "mysql",
                        "source-database",
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("hostname", "localhost2")
                                        .put("port", "3001")
                                        .put("username", "admin")
                                        .put("password", "password1")
                                        .put("tables", "accum_[0-9]+.accumulation")
                                        .put("server-id", "5400-5404")
                                        .put("server-time-zone", "Asia/Shanghai")
                                        .build()));
        sourceDefs.add(sourceDef1);
        sourceDefs.add(sourceDef2);
        PipelineDef udalDecryptedDefWithDERKey =
                new PipelineDef(
                        sourceDefs,
                        new SinkDef(
                                "doris",
                                "sink-queue",
                                Configuration.fromMap(
                                        ImmutableMap.<String, String>builder()
                                                .put("fenodes", "localhost:8035")
                                                .put("username", "root")
                                                .put("password", "password2")
                                                .put(
                                                        "table.create.properties.light_schema_change",
                                                        "true")
                                                .put("table.create.properties.replication_num", "1")
                                                .build())),
                        Arrays.asList(
                                new RouteDef(
                                        "accum_[0-9]+.accumulation",
                                        "accum.accumulation",
                                        "sync table to one destination table")),
                        null,
                        Configuration.fromMap(
                                        ImmutableMap.<String, String>builder()
                                                .put("name", "Sync MySQL Database to Doris")
                                                .put("parallelism", "2")
                                                .put("jasypt-encryptor.private-key-format", "DER")
                                                .put(
                                                        "jasypt-encryptor.private-key-location",
                                                        "src/test/resources/private_key.der")
                                                .put("shade.identifier", "rsa")
                                                .put("shade.sensitive.keywords", "password")
                                                .build())
                                .set(
                                        PipelineOptions.ADDITIONAL_FLINK_CONF,
                                        ImmutableMap.of(
                                                SECRET_ENCRYPTOR_PRIVATE_KEY_STRING.key(),
                                                "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCVx0Qtu1PTzSGc7Uwg96NvgttYzNzjouaBeoQwzwBm0QpynitoehJMAyBLxqX4Z7eyqEUVem5oIkKYPxiNGVsEBOda0r/u1KxD5bmq0mdVF3xDKPY7j/ogrA//mg7wZPU6fMZPmzea585d4GPMC9N78L0I+N9Plq3BisPoltx5JfBkXdvbvS6AE72raNEO24TPvcPYgXaIuL2h2uF4oX6D8kmsnA1hGuz2gT1ZncIgCVdfM5FaNppsKLKM1CKOq9Xjhe2wKDThYnzDPvhx0NDfp/AnvEUUQB/ZKJYnqzZpEt25s8bokB5BBhZHGrpzOx1RvF5N0Olx2HOxmxPeNEfrAgMBAAECggEAXEE7xmtfC9OLA6zKOnguC+5vCvhMik35awE60RF/rtTNeaHhHN4rzPQT/XijHClOZdXtj3g07yFDIaGjjq1yeTxIAJJVNFd8r+mc/hHRIgFwQbHGUROyvlMdG5QZ5YpZ3ieiwr/ZdSmY3AvtjG4wWmQDG7anXc9ywQam+umJbXi2qrfikPkUteLK7/JLLVpOYSEdNxKvpi99ehOKvIvfOXZdzsemsDKso2M7M6m/50JThfgM4Y112H8D77SeGGCoS0JjqEH+Y9veFhIGNjMkEKo0+mCmdC3Zucc5SpNHWUJbsZ6NnMJK2vdDzMCvW2OAM16K6VQdrp1cNJFT8fZGuQKBgQDD/gH4C9J4QKjphdgO/Tc7MNfuscIZSvNxcv/KNeCcjWMiNNp98W1yLDkjqvZKsVLtT4xrvLJLyz/425GFGqwZdrnuPLJ4KDYAideYlaeYGHkrg3GgkvDdXw5XNkZzl2OgwS42miOI3jrLgDxETkWdm5GOlFFYqeLcWMHaA40LbQKBgQDDov1Rpz4gtty3UylhH7Rvf2HWmJpMTzj0lHh4ctBvKvbVs61O06kPeT2M1mrq6rZsCbT9+AMo8H5Ollkv1BxYlhplWOCS2Wn2kIbl15LbD1nOUm8NUT455hqbXqwQAHuNfJHFLRK6lDpQOW5HzEUFf9NrYP+ElmXWCu38X+5xtwKBgQCy7NNzVaehbLZH6o7isDyZ5u9a1CE04f35Vlk6i5Ewmssj0UieraeTEdhgPZV3fwcL6xFw0eWb2EPgtuLUtxLidEctvW/YzizutOiEPiuwaLdGCEvVohAOqNb4u2353P1xJqs/4dwK+YaHfdyHJ0XaqslpdvIElaOsMcNXeuJqyQKBgDm27dCW7CcuizKyH/T9K3fxNmXeozZ78KuG2Xt/M6p4HFWzKh+lJazg8Z2I7AXdtG9u9awu7I+5UynQBQOtKaegsKzRaX8pEK+it4k67aIehzns68IcA6WFErhfV5do7Qoyg7aNs4bDj+h26OfZhUs4CKEW/oqY6/YXR1o62tdFAoGBAK2AkTkGLWksxIpAMJ+GwPwxIlK6UOtoyxVefIKEy4bVt4vQZFLXwsKKzSTZX5z9CDzV/MUp51Gao9eo0TJkdtjUhNHGV4jdIUzB07V3BoL7nenxNk7Es7Hrp4Jljm8QgILtaeTIDd9pq7GciDi2WHsnD9Ipc1pNK8Rj0+8I8Pdx")));
        return udalDecryptedDefWithDERKey;
    }
}
