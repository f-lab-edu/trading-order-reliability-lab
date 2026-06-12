package com.trading.orderreliability.order.adapter.out.persistence.conversion;

import com.trading.orderreliability.common.id.UuidBytes;

import java.util.UUID;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
class UuidBinaryConverter implements AttributeConverter<UUID, byte[]> {

    @Override
    public byte[] convertToDatabaseColumn(UUID attribute) {
        return attribute == null ? null : UuidBytes.toBytes(attribute);
    }

    @Override
    public UUID convertToEntityAttribute(byte[] dbData) {
        return dbData == null ? null : UuidBytes.fromBytes(dbData);
    }
}
