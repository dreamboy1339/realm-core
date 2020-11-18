#ifndef REALM_OBJECT_STORE_C_API_CONVERSION_HPP
#define REALM_OBJECT_STORE_C_API_CONVERSION_HPP

#include <realm/realm.h>

#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/collection.hpp>

#include <realm/string_data.hpp>
#include <realm/binary_data.hpp>
#include <realm/timestamp.hpp>
#include <realm/decimal128.hpp>
#include <realm/object_id.hpp>
#include <realm/mixed.hpp>
#include <realm/uuid.hpp>

#include <string>

namespace realm {

static inline realm_string_t to_capi(StringData data)
{
    return realm_string_t{data.data(), data.size()};
}

static inline realm_string_t to_capi(const std::string& str)
{
    return to_capi(StringData{str});
}

static inline std::string capi_to_std(realm_string_t str)
{
    if (str.data) {
        return std::string{str.data, 0, str.size};
    }
    return std::string{};
}

static inline StringData from_capi(realm_string_t str)
{
    return StringData{str.data, str.size};
}

static inline realm_binary_t to_capi(BinaryData bin)
{
    return realm_binary_t{reinterpret_cast<const unsigned char*>(bin.data()), bin.size()};
}

static inline BinaryData from_capi(realm_binary_t bin)
{
    return BinaryData{reinterpret_cast<const char*>(bin.data), bin.size};
}

static inline realm_timestamp_t to_capi(Timestamp ts)
{
    return realm_timestamp_t{ts.get_seconds(), ts.get_nanoseconds()};
}

static inline Timestamp from_capi(realm_timestamp_t ts)
{
    return Timestamp{ts.seconds, ts.nanoseconds};
}

static inline realm_decimal128_t to_capi(const Decimal128& dec)
{
    auto raw = dec.raw();
    return realm_decimal128_t{{raw->w[0], raw->w[1]}};
}

static inline Decimal128 from_capi(realm_decimal128_t dec)
{
    return Decimal128{Decimal128::Bid128{{dec.w[0], dec.w[1]}}};
}

static inline realm_object_id_t to_capi(ObjectId)
{
    REALM_TERMINATE("Not implemented yet.");
}

static inline ObjectId from_capi(realm_object_id_t)
{
    REALM_TERMINATE("Not implemented yet.");
}

static inline realm_col_key_t to_capi(ColKey key)
{
    return realm_col_key_t{key.value};
}

static inline ColKey from_capi(realm_col_key_t key)
{
    return ColKey{key.col_key};
}

static inline realm_table_key_t to_capi(TableKey key)
{
    return realm_table_key_t{key.value};
}

static inline TableKey from_capi(realm_table_key_t key)
{
    return TableKey{key.table_key};
}

static inline realm_obj_key_t to_capi(ObjKey key)
{
    return realm_obj_key_t{key.value};
}

static inline ObjKey from_capi(realm_obj_key_t key)
{
    return ObjKey{key.obj_key};
}

static inline ObjLink from_capi(realm_link_t val)
{
    return ObjLink{from_capi(val.target_table), from_capi(val.target)};
}

static inline realm_link_t to_capi(ObjLink link)
{
    return realm_link_t{to_capi(link.get_table_key()), to_capi(link.get_obj_key())};
}

static inline UUID from_capi(realm_uuid_t val)
{
    static_assert(sizeof(val.bytes) == UUID::num_bytes);
    UUID::UUIDBytes bytes;
    std::copy(val.bytes, val.bytes + UUID::num_bytes, bytes.data());
    return UUID{bytes};
}

static inline realm_uuid_t to_capi(UUID val)
{
    realm_uuid_t uuid;
    auto bytes = val.to_bytes();
    std::copy(bytes.data(), bytes.data() + UUID::num_bytes, uuid.bytes);
    return uuid;
}

static inline Mixed from_capi(realm_value_t val)
{
    switch (val.type) {
        case RLM_TYPE_NULL:
            return Mixed{};
        case RLM_TYPE_INT:
            return Mixed{val.integer};
        case RLM_TYPE_BOOL:
            return Mixed{val.boolean};
        case RLM_TYPE_STRING:
            return Mixed{from_capi(val.string)};
        case RLM_TYPE_BINARY:
            return Mixed{from_capi(val.binary)};
        case RLM_TYPE_TIMESTAMP:
            return Mixed{from_capi(val.timestamp)};
        case RLM_TYPE_FLOAT:
            return Mixed{val.fnum};
        case RLM_TYPE_DOUBLE:
            return Mixed{val.dnum};
        case RLM_TYPE_DECIMAL128:
            return Mixed{from_capi(val.decimal128)};
        case RLM_TYPE_OBJECT_ID:
            return Mixed{from_capi(val.object_id)};
        case RLM_TYPE_LINK:
            return Mixed{ObjLink{from_capi(val.link.target_table), from_capi(val.link.target)}};
        case RLM_TYPE_UUID:
            return Mixed{UUID{from_capi(val.uuid)}};
    }
    REALM_TERMINATE("Invalid realm_value_t");
}

static inline realm_value_t to_capi(Mixed value)
{
    realm_value_t val;
    if (value.is_null()) {
        val.type = RLM_TYPE_NULL;
    }
    else {
        switch (value.get_type()) {
            case type_Int: {
                val.type = RLM_TYPE_INT;
                val.integer = value.get<int64_t>();
                break;
            }
            case type_Bool: {
                val.type = RLM_TYPE_BOOL;
                val.boolean = value.get<bool>();
                break;
            }
            case type_String: {
                val.type = RLM_TYPE_STRING;
                val.string = to_capi(value.get<StringData>());
                break;
            }
            case type_Binary: {
                val.type = RLM_TYPE_BINARY;
                val.binary = to_capi(value.get<BinaryData>());
                break;
            }
            case type_Timestamp: {
                val.type = RLM_TYPE_TIMESTAMP;
                val.timestamp = to_capi(value.get<Timestamp>());
                break;
            }
            case type_Float: {
                val.type = RLM_TYPE_FLOAT;
                val.fnum = value.get<float>();
                break;
            }
            case type_Double: {
                val.type = RLM_TYPE_DOUBLE;
                val.dnum = value.get<double>();
                break;
            }
            case type_Decimal: {
                val.type = RLM_TYPE_DECIMAL128;
                val.decimal128 = to_capi(value.get<Decimal128>());
                break;
            }
            case type_Link: {
                REALM_TERMINATE("Not implemented yet");
            }
            case type_ObjectId: {
                val.type = RLM_TYPE_OBJECT_ID;
                val.object_id = to_capi(value.get<ObjectId>());
                break;
            }
            case type_TypedLink: {
                val.type = RLM_TYPE_LINK;
                auto link = value.get<ObjLink>();
                val.link.target_table = to_capi(link.get_table_key());
                val.link.target = to_capi(link.get_obj_key());
                break;
            }
            case type_UUID: {
                val.type = RLM_TYPE_UUID;
                auto uuid = value.get<UUID>();
                val.uuid = to_capi(uuid);
                break;
            }

            case type_LinkList:
                [[fallthrough]];
            case type_Mixed:
                [[fallthrough]];
            case type_OldTable:
                [[fallthrough]];
            case type_OldDateTime:
                [[fallthrough]];
            default:
                REALM_TERMINATE("Invalid Mixed value type");
        }
    }

    return val;
}

static inline SchemaMode from_capi(realm_schema_mode_e mode)
{
    switch (mode) {
        case RLM_SCHEMA_MODE_AUTOMATIC: {
            return SchemaMode::Automatic;
        }
        case RLM_SCHEMA_MODE_IMMUTABLE: {
            return SchemaMode::Immutable;
        }
        case RLM_SCHEMA_MODE_READ_ONLY_ALTERNATIVE: {
            return SchemaMode::ReadOnlyAlternative;
        }
        case RLM_SCHEMA_MODE_RESET_FILE: {
            return SchemaMode::ResetFile;
        }
        case RLM_SCHEMA_MODE_ADDITIVE: {
            return SchemaMode::Additive;
        }
        case RLM_SCHEMA_MODE_MANUAL: {
            return SchemaMode::Manual;
        }
    }
    REALM_TERMINATE("Invalid schema mode.");
}

static inline realm_property_type_e to_capi(PropertyType type) noexcept
{
    type &= ~PropertyType::Flags;

    switch (type) {
        case PropertyType::Int:
            return RLM_PROPERTY_TYPE_INT;
        case PropertyType::Bool:
            return RLM_PROPERTY_TYPE_BOOL;
        case PropertyType::String:
            return RLM_PROPERTY_TYPE_STRING;
        case PropertyType::Data:
            return RLM_PROPERTY_TYPE_BINARY;
        case PropertyType::Mixed:
            return RLM_PROPERTY_TYPE_MIXED;
        case PropertyType::Date:
            return RLM_PROPERTY_TYPE_TIMESTAMP;
        case PropertyType::Float:
            return RLM_PROPERTY_TYPE_FLOAT;
        case PropertyType::Double:
            return RLM_PROPERTY_TYPE_DOUBLE;
        case PropertyType::Decimal:
            return RLM_PROPERTY_TYPE_DECIMAL128;
        case PropertyType::Object:
            return RLM_PROPERTY_TYPE_OBJECT;
        case PropertyType::LinkingObjects:
            return RLM_PROPERTY_TYPE_LINKING_OBJECTS;
        case PropertyType::ObjectId:
            return RLM_PROPERTY_TYPE_OBJECT_ID;
        case PropertyType::UUID:
            return RLM_PROPERTY_TYPE_UUID;
        case PropertyType::Nullable:
            [[fallthrough]];
        case PropertyType::Flags:
            [[fallthrough]];
        case PropertyType::Set:
            [[fallthrough]];
        case PropertyType::Dictionary:
            [[fallthrough]];
        case PropertyType::Collection:
            [[fallthrough]];
        case PropertyType::Array:
            REALM_UNREACHABLE();
    }
    REALM_TERMINATE("Unsupported property type");
}

static inline PropertyType from_capi(realm_property_type_e type) noexcept
{
    switch (type) {
        case RLM_PROPERTY_TYPE_INT:
            return PropertyType::Int;
        case RLM_PROPERTY_TYPE_BOOL:
            return PropertyType::Bool;
        case RLM_PROPERTY_TYPE_STRING:
            return PropertyType::String;
        case RLM_PROPERTY_TYPE_BINARY:
            return PropertyType::Data;
        case RLM_PROPERTY_TYPE_MIXED:
            return PropertyType::Mixed;
        case RLM_PROPERTY_TYPE_TIMESTAMP:
            return PropertyType::Date;
        case RLM_PROPERTY_TYPE_FLOAT:
            return PropertyType::Float;
        case RLM_PROPERTY_TYPE_DOUBLE:
            return PropertyType::Double;
        case RLM_PROPERTY_TYPE_DECIMAL128:
            return PropertyType::Decimal;
        case RLM_PROPERTY_TYPE_OBJECT:
            return PropertyType::Object;
        case RLM_PROPERTY_TYPE_LINKING_OBJECTS:
            return PropertyType::LinkingObjects;
        case RLM_PROPERTY_TYPE_OBJECT_ID:
            return PropertyType::ObjectId;
        case RLM_PROPERTY_TYPE_UUID:
            return PropertyType::UUID;
    }
    REALM_TERMINATE("Unsupported property type");
}


static inline Property from_capi(const realm_property_info_t& p) noexcept
{
    Property prop;
    prop.name = capi_to_std(p.name);
    prop.public_name = capi_to_std(p.public_name);
    prop.type = from_capi(p.type);
    prop.object_type = capi_to_std(p.link_target);
    prop.link_origin_property_name = capi_to_std(p.link_origin_property_name);
    prop.is_primary = Property::IsPrimary{bool(p.flags & RLM_PROPERTY_PRIMARY_KEY)};
    prop.is_indexed = Property::IsIndexed{bool(p.flags & RLM_PROPERTY_INDEXED)};

    if (bool(p.flags & RLM_PROPERTY_NULLABLE)) {
        prop.type |= PropertyType::Nullable;
    }
    switch (p.collection_type) {
        case RLM_COLLECTION_TYPE_NONE:
            break;
        case RLM_COLLECTION_TYPE_LIST: {
            prop.type |= PropertyType::Array;
            break;
        }
        case RLM_COLLECTION_TYPE_SET: {
            prop.type |= PropertyType::Set;
            break;
        }
        case RLM_COLLECTION_TYPE_DICTIONARY: {
            prop.type |= PropertyType::Dictionary;
            break;
        }
    }
    return prop;
}

static inline realm_property_info_t to_capi(const Property& prop) noexcept
{
    realm_property_info_t p;
    p.name = to_capi(prop.name);
    p.public_name = to_capi(prop.public_name);
    p.type = to_capi(prop.type & ~PropertyType::Flags);
    p.link_target = to_capi(prop.object_type);
    p.link_origin_property_name = to_capi(prop.link_origin_property_name);

    p.flags = RLM_PROPERTY_NORMAL;
    if (prop.is_indexed)
        p.flags |= RLM_PROPERTY_INDEXED;
    if (prop.is_primary)
        p.flags |= RLM_PROPERTY_PRIMARY_KEY;
    if (bool(prop.type & PropertyType::Nullable))
        p.flags |= RLM_PROPERTY_NULLABLE;

    p.collection_type = RLM_COLLECTION_TYPE_NONE;
    if (bool(prop.type & PropertyType::Array))
        p.collection_type = RLM_COLLECTION_TYPE_LIST;
    if (bool(prop.type & PropertyType::Set))
        p.collection_type = RLM_COLLECTION_TYPE_SET;
    if (bool(prop.type & PropertyType::Dictionary))
        p.collection_type = RLM_COLLECTION_TYPE_DICTIONARY;

    p.key = to_capi(prop.column_key);

    return p;
}

static inline realm_class_info_t to_capi(const ObjectSchema& o)
{
    realm_class_info_t info;
    info.name = to_capi(o.name);
    info.primary_key = to_capi(o.primary_key);
    info.num_properties = o.persisted_properties.size();
    info.num_computed_properties = o.computed_properties.size();
    info.key = to_capi(o.table_key);
    if (o.is_embedded) {
        info.flags = RLM_CLASS_EMBEDDED;
    }
    else {
        info.flags = RLM_CLASS_NORMAL;
    }
    return info;
}

/// Convert a Mixed that potentially contains an untyped ObjKey to a Mixed
/// containing an ObjLink.
static inline Mixed resolve_untyped_link(Mixed value, const Table& source_table, ColKey source_column)
{
    if (!value.is_null() && value.get_type() == type_Link) {
        REALM_ASSERT(source_column.get_type() == col_type_LinkList || source_column.get_type() == col_type_Link);

        auto target_table = source_table.get_link_target(source_column);
        value = ObjLink{target_table->get_key(), value.get<ObjKey>()};
    }
    return value;
}

/// Convert a Mixed that potentially contains an untyped ObjKey to a Mixed
/// containing an ObjLink.
static inline Mixed resolve_untyped_link(Mixed value, const object_store::Collection& coll)
{
    auto& realm = *coll.get_realm();
    auto source_table = realm.read_group().get_table(coll.get_parent_table_key());
    auto source_column = coll.get_parent_column_key();
    return resolve_untyped_link(value, *source_table, source_column);
}

/// Convert a Mixed that potentially contains an ObjLink to a Mixed containing
/// an untyped ObjKey.
static inline Mixed typed_link_to_link(Mixed value)
{
    if (!value.is_null() && value.get_type() == type_TypedLink) {
        auto link = value.get<ObjLink>();
        value = link.get_obj_key();
    }
    return value;
}

static inline Mixed typed_link_to_link_checked(Mixed value, const Table& source_table, ColKey source_column)
{
    if (!value.is_null() && value.get_type() == type_TypedLink) {
        if (source_column.get_type() == col_type_Link || source_column.get_type() == col_type_LinkList) {
            auto link = value.get<ObjLink>();

            if (link.get_table_key() != source_table.get_link_target(source_column)->get_key()) {
                // FIXME: Better exception
                throw std::invalid_argument{"Type mismatch in target table"};
            }

            return link.get_obj_key();
        }
    }

    return value;
}

static inline Mixed typed_link_to_link_checked(Mixed value, const object_store::Collection& coll)
{
    auto& realm = *coll.get_realm();
    auto source_table = realm.read_group().get_table(coll.get_parent_table_key());
    auto source_column = coll.get_parent_column_key();
    return typed_link_to_link_checked(value, *source_table, source_column);
}

/// For a Mixed value and a property, check that the type matches, and call `f`
/// with the value of the Mixed if it does.
///
/// For links, this also converts ObjKey/ObjLink to Obj.
///
/// FIXME: This should move into Object Store as it gains better support for
/// Mixed (similar to Core's `insert_any()` etc.).
template <class F>
static auto switch_on_mixed_checked(const std::shared_ptr<Realm>& realm, PropertyType val_type, Mixed val, F&& f)
{
    // FIXME: Object Store has poor support for heterogeneous lists, and in
    // particular it relies on Core to check that the input types to
    // `List::insert()` etc. match the list property type. Once that is fixed /
    // made safer, this logic should move into Object Store.

    if (val.is_null()) {
        if (!is_nullable(val_type)) {
            // FIXME: Defer this exception to Object Store, which can produce
            // nicer message.
            throw std::invalid_argument("NULL in non-nullable field/list.");
        }

        // Produce a util::none matching the property type.
        return switch_on_type(val_type, [&](auto ptr) {
            using T = std::remove_cv_t<std::remove_pointer_t<decltype(ptr)>>;
            T nothing{};
            return f(nothing);
        });
    }

    PropertyType base_type = (val_type & ~PropertyType::Flags);

    // Note: The following checks PropertyType::Mixed on the assumption that it
    // will become un-deprecated when Mixed is exposed in Object Store.

    switch (val.get_type()) {
        case type_Int: {
            if (base_type != PropertyType::Int && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<int64_t>());
        }
        case type_Bool: {
            if (base_type != PropertyType::Bool && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<bool>());
        }
        case type_String: {
            if (base_type != PropertyType::String && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<StringData>());
        }
        case type_Binary: {
            if (base_type != PropertyType::Data && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<BinaryData>());
        }
        case type_Timestamp: {
            if (base_type != PropertyType::Date && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<Timestamp>());
        }
        case type_Float: {
            if (base_type != PropertyType::Float && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<float>());
        }
        case type_Double: {
            if (base_type != PropertyType::Double && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<double>());
        }
        case type_Decimal: {
            if (base_type != PropertyType::Decimal && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<Decimal128>());
        }
        case type_ObjectId: {
            if (base_type != PropertyType::ObjectId && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<ObjectId>());
        }
        case type_TypedLink: {
            if (base_type != PropertyType::Object && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            // Object Store performs link validation already. Just create an Obj
            // for the link, and pass it on.
            auto link = val.get<ObjLink>();
            auto target_table = realm->read_group().get_table(link.get_table_key());
            auto obj = target_table->get_object(link.get_obj_key());
            return f(std::move(obj));
        }
        case type_UUID: {
            if (base_type != PropertyType::UUID && base_type != PropertyType::Mixed)
                throw std::invalid_argument{"Type mismatch"};
            return f(val.get<UUID>());
        }

        case type_Link:
            // Note: from_capi(realm_value_t) never produces an untyped link.
            [[fallthrough]];
        case type_OldTable:
            [[fallthrough]];
        case type_Mixed:
            [[fallthrough]];
        case type_OldDateTime:
            [[fallthrough]];
        case type_LinkList:
            REALM_TERMINATE("Invalid value type.");
    }
}

} // namespace realm


#endif // REALM_OBJECT_STORE_C_API_CONVERSION_HPP