#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/util/overload.hpp>

RLM_API bool realm_dictionary_size(const realm_dictionary_t* dict, size_t* out_size)
{
    return wrap_err([&]() {
        size_t size = dict->size();
        if (out_size)
            *out_size = size;
        return true;
    });
}


RLM_API bool realm_dictionary_get(const realm_dictionary_t* dict, realm_value_t key, realm_value_t* out_value,
                                  bool* out_found)
{
    return wrap_err([&]() {
        if (key.type != RLM_TYPE_STRING) {
            throw std::invalid_argument{"Non-string dictionary keys are not supported."};
        }

        auto mixed_key = from_capi(key);
        REALM_ASSERT(!mixed_key.is_null() && mixed_key.get_type() == type_String);
        auto string_key = mixed_key.get<StringData>();

        realm_value_t result;
        auto getter = util::overload{
            [&](Obj*) {
                Obj o = dict->get<Obj>(string_key);
                result.type = RLM_TYPE_LINK;
                result.link.target_table = to_capi(o.get_table()->get_key());
                result.link.target = to_capi(o.get_key());
            },
            [&](auto p) {
                using T = std::remove_cv_t<std::remove_pointer_t<decltype(p)>>;
                Mixed mixed{dict->get<T>(string_key)};
                result = to_capi(mixed);
            },
        };

        try {
            switch_on_type(dict->get_type(), getter);
            if (out_value)
                *out_value = result;
            if (out_found)
                *out_found = true;
        }
        catch (const KeyNotFound&) {
            if (out_found)
                *out_found = false;
            if (out_value)
                out_value->type = RLM_TYPE_NULL;
        }

        return true;
    });
}

RLM_API bool realm_dictionary_insert(realm_dictionary_t* dict, realm_value_t key, realm_value_t value,
                                     bool* out_inserted)
{
    return wrap_err([&]() {
        if (key.type != RLM_TYPE_STRING) {
            throw std::invalid_argument{"Non-string dictionary keys are not supported."};
        }

        auto mixed_key = from_capi(key);
        REALM_ASSERT(!mixed_key.is_null() && mixed_key.get_type() == type_String);
        auto string_key = mixed_key.get<StringData>();

        auto val = from_capi(value);
        switch_on_mixed_checked(dict->get_realm(), dict->get_type(), val, [&](auto val) {
            dict->insert(string_key, val);
        });

        if (out_inserted)
            *out_inserted = true;

        return true;
    });
}

RLM_API bool realm_dictionary_erase(realm_dictionary_t* dict, realm_value_t key, bool* out_erased)
{
    return wrap_err([&]() {
        if (key.type != RLM_TYPE_STRING) {
            throw std::invalid_argument{"Non-string dictionary keys are not supported."};
        }

        auto mixed_key = from_capi(key);
        REALM_ASSERT(!mixed_key.is_null() && mixed_key.get_type() == type_String);
        auto string_key = mixed_key.get<StringData>();

        dict->erase(string_key);

        if (out_erased)
            *out_erased = true; // FIXME

        return true;
    });
}

RLM_API bool realm_dictionary_clear(realm_dictionary_t* dict)
{
    return wrap_err([&]() {
        dict->remove_all();
        return true;
    });
}
