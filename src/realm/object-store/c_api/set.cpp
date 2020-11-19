#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

#include <realm/util/overload.hpp>

RLM_API bool realm_set_size(const realm_set_t* set, size_t* out_size)
{
    return wrap_err([&]() {
        size_t size = set->size();
        if (out_size)
            *out_size = size;
        return true;
    });
}
