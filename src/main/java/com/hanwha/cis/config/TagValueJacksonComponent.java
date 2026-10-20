package com.hanwha.cis.config;

import com.hanwha.cis.domain.model.TagValue;
import org.springframework.boot.jackson.JacksonComponent;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;

/**
 * {@link TagValue}를 JSON 원시 값(true/false/숫자)으로 읽고 쓴다. 도메인에는 Jackson 어노테이션을 두지 않으므로 여기서 등록한다.
 * Boot가 만드는 JsonMapper(REST, Redis, 이력 JSON, STOMP)에 모두 적용된다.
 */
@JacksonComponent
public class TagValueJacksonComponent {

    public static class Serializer extends ValueSerializer<TagValue> {
        @Override
        public void serialize(TagValue value, JsonGenerator gen, SerializationContext ctxt) {
            switch (value) {
                case TagValue.BoolValue b -> gen.writeBoolean(b.value());
                case TagValue.NumberValue n -> {
                    if (n.isIntegral()) {
                        gen.writeNumber((long) n.value());
                    } else {
                        gen.writeNumber(n.value());
                    }
                }
            }
        }
    }

    public static class Deserializer extends ValueDeserializer<TagValue> {
        @Override
        public TagValue deserialize(JsonParser p, DeserializationContext ctxt) {
            return switch (p.currentToken()) {
                case VALUE_TRUE -> new TagValue.BoolValue(true);
                case VALUE_FALSE -> new TagValue.BoolValue(false);
                case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> new TagValue.NumberValue(p.getDoubleValue());
                case VALUE_STRING -> TagValue.of(p.getString());
                default -> (TagValue) ctxt.handleUnexpectedToken(TagValue.class, p);
            };
        }
    }
}
