#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/util/overload.hpp>

RLM_API bool realm_list_size(const realm_list_t* list, size_t* out_size)
{
    return wrap_err([&]() {
        size_t size = list->size();
        if (out_size)
            *out_size = size;
        return true;
    });
}

RLM_API bool realm_list_get_property(const realm_list_t* list, realm_property_info_t* out_property_info)
{
    static_cast<void>(list);
    static_cast<void>(out_property_info);
    REALM_TERMINATE("Not implemented yet.");
}

/// Convert a Mixed that potentially contains an ObjKey from a link list to a
/// Mixed containing an ObjLink.
static inline Mixed link_to_typed_link(Mixed value, const List& list)
{
    if (!value.is_null() && value.get_type() == type_Link) {
        auto col_key = list.get_parent_column_key();
        REALM_ASSERT(col_key.get_type() == col_type_LinkList ||
                     (col_key.get_type() == col_type_Link && col_key.is_list()));
        REALM_ASSERT(list.get_type() == (PropertyType::Object | PropertyType::Array));

        // Get the target table key.
        auto& shared_realm = *list.get_realm();
        auto source_table = shared_realm.read_group().get_table(list.get_parent_table_key());
        auto target_table = source_table->get_link_target(col_key);
        value = ObjLink{target_table->get_key(), value.get<ObjKey>()};
    }
    return value;
}

/// Convert a Mixed that potentially contains an ObjLink to a Mixed containing an ObjKey.
static inline Mixed typed_link_to_link(Mixed value)
{
    if (!value.is_null() && value.get_type() == type_TypedLink) {
        auto link = value.get<ObjLink>();
        value = link.get_obj_key();
    }
    return value;
}

RLM_API bool realm_list_get(const realm_list_t* list, size_t index, realm_value_t* out_value)
{
    return wrap_err([&]() {
        list->verify_attached();
        realm_value_t result;

        auto getter = util::overload{
            [&](Obj*) {
                Obj o = list->get<Obj>(index);
                result.type = RLM_TYPE_LINK;
                result.link.target_table = to_capi(o.get_table()->get_key());
                result.link.target = to_capi(o.get_key());
            },
            [&](util::Optional<Obj>*) {
                REALM_TERMINATE("Nullable link lists not supported");
            },
            [&](auto p) {
                using T = std::remove_cv_t<std::remove_pointer_t<decltype(p)>>;
                Mixed mixed{list->get<T>(index)};
                result = to_capi(mixed);
            },
        };

        switch_on_type(list->get_type(), getter);

        if (out_value)
            *out_value = result;
        return true;
    });
}

template <class F>
auto value_or_object(const std::shared_ptr<Realm>& realm, PropertyType val_type, Mixed val, F&& f)
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

RLM_API bool realm_list_insert(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        value_or_object(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->insert(index, val);
        });
        return true;
    });
}

RLM_API bool realm_list_set(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        value_or_object(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->set(index, val);
        });
        return true;
    });
}

RLM_API bool realm_list_erase(realm_list_t* list, size_t index)
{
    return wrap_err([&]() {
        list->remove(index);
        return true;
    });
}

RLM_API bool realm_list_clear(realm_list_t* list)
{
    return wrap_err([&]() {
        list->remove_all();
        return true;
    });
}

RLM_API realm_list_t* realm_list_from_thread_safe_reference(const realm_t* realm, realm_thread_safe_reference_t* tsr)
{
    return wrap_err([&]() {
        auto ltsr = dynamic_cast<realm_list::thread_safe_reference*>(tsr);
        if (!ltsr) {
            throw std::logic_error{"Thread safe reference type mismatch"};
        }

        auto list = ltsr->resolve<List>(*realm);
        return new realm_list_t{std::move(list)};
    });
}