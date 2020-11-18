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


RLM_API bool realm_list_insert(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        switch_on_mixed_checked(list->get_realm(), list->get_type(), val, [&](auto val) {
            list->insert(index, val);
        });
        return true;
    });
}

RLM_API bool realm_list_set(realm_list_t* list, size_t index, realm_value_t value)
{
    return wrap_err([&]() {
        auto val = from_capi(value);
        switch_on_mixed_checked(list->get_realm(), list->get_type(), val, [&](auto val) {
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