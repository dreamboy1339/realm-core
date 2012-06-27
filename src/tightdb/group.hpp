/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/

#ifndef TIGHTDB_GROUP_HPP
#define TIGHTDB_GROUP_HPP

#include "static_assert.hpp"
#include "table.hpp"
#include "table_basic_fwd.hpp"
#include "alloc_slab.hpp"

namespace tightdb {


// Pre-declarations
class SharedGroup;

enum GroupMode {
    GROUP_DEFAULT  =  0,
    GROUP_READONLY =  1,
    GROUP_SHARED   =  2,
    GROUP_APPEND   =  4,
    GROUP_ASYNC    =  8,
    GROUP_SWAPONLY = 16
};

class Group: private Table::Parent {
public:
    Group();
    Group(const char* filename, int mode=GROUP_DEFAULT);
    Group(const char* buffer, size_t len);
    ~Group();

    bool is_valid() const {return m_isValid;}
    bool is_shared() const {return m_persistMode & GROUP_SHARED;}
    bool is_empty() const;

    size_t get_table_count() const;
    const char* get_table_name(size_t table_ndx) const;
    bool has_table(const char* name) const;
    // FIXME: Add has_table<MyTable>(name) which also checks the spec

    TableRef      get_table(const char* name);
    ConstTableRef get_table(const char* name) const;
    template<class T> typename T::Ref      get_table(const char* name);
    template<class T> typename T::ConstRef get_table(const char* name) const;

    // Serialization
    bool write(const char* filepath);
    char* write_to_mem(size_t& len);

    bool commit();

    // Conversion
    template<class S> void to_json(S& out) const;

#ifdef _DEBUG
    void Verify(); // Must be upper case to avoid conflict with macro in ObjC
    void print() const;
    void print_free() const;
    MemStats stats();
    void enable_mem_diagnostics(bool enable=true) {m_alloc.EnableDebug(enable);}
    void to_dot(std::ostream& out) const;
    void to_dot() const; // For GDB
    void zero_free_space(size_t file_size, size_t readlock_version);
#endif //_DEBUG

protected:
    friend class GroupWriter;
    friend class SharedGroup;

    void init_shared();
    bool commit(size_t current_version, size_t readlock_version);
    void rollback();

#ifdef TIGHTDB_ENABLE_REPLICATION
    void set_replication(Replication* r) { m_alloc.set_replication(r); }
#endif

    SlabAlloc& get_allocator() {return m_alloc;}
    Array& get_top_array() {return m_top;}

    // Recursively update all internal refs after commit
    void update_refs(size_t top_ref);

    void update_from_shared(size_t top_ref, size_t len);
    void reset_to_new();

    // Overriding method in ArrayParent
    virtual void update_child_ref(size_t subtable_ndx, size_t new_ref)
    {
        m_tables.Set(subtable_ndx, new_ref);
    }

    // Overriding method in Table::Parent
    virtual void child_destroyed(std::size_t) {} // Ignore

    // Overriding method in ArrayParent
    virtual size_t get_child_ref(size_t subtable_ndx) const
    {
        return m_tables.GetAsRef(subtable_ndx);
    }

    void create();
    void create_from_ref();

    Table* get_table_ptr(size_t ndx);
    const Table* get_table_ptr(size_t ndx) const;

    template<class S> size_t write(S& out);

    // Member variables
    SlabAlloc m_alloc;
    Array m_top;
    Array m_tables;
    ArrayString m_tableNames;
    Array m_freePositions;
    Array m_freeLengths;
    Array m_freeVersions;
    mutable Array m_cachedtables;
    uint32_t m_persistMode;
    size_t m_readlock_version;
    bool m_isValid;

private:
    Table* get_table_ptr(const char* name);
    const Table* get_table_ptr(const char* name) const;
    template<class T> T* get_table_ptr(const char* name);
    template<class T> const T* get_table_ptr(const char* name) const;

    Table* create_new_table(const char* name);

    friend class LangBindHelper;
};



// Implementation

inline const Table* Group::get_table_ptr(size_t ndx) const
{
    return const_cast<Group*>(this)->get_table_ptr(ndx);
}

inline Table* Group::get_table_ptr(const char* name)
{
    assert(m_top.IsValid());
    const size_t ndx = m_tableNames.find_first(name);
    if (ndx != size_t(-1)) {
        // Get table from cache
        return get_table_ptr(ndx);
    }

    return create_new_table(name);
}

inline const Table* Group::get_table_ptr(const char* name) const
{
    assert(has_table(name));
    return const_cast<Group*>(this)->get_table_ptr(name);
}

template<class T> inline T* Group::get_table_ptr(const char* name)
{
    TIGHTDB_STATIC_ASSERT(IsBasicTable<T>::value, "Invalid table type");
    // FIXME: assert(!has_table(name) || has_table<T>(name));

    assert(m_top.IsValid());
    const size_t ndx = m_tableNames.find_first(name);
    if (ndx != size_t(-1)) {
        // Get table from cache
        return static_cast<T*>(get_table_ptr(ndx));
    }

    T* const table = static_cast<T*>(create_new_table(name));
    table->set_dynamic_spec();
    return table;
}

template<class T> inline const T* Group::get_table_ptr(const char* name) const
{
    assert(has_table(name));
    return const_cast<Group*>(this)->get_table_ptr<T>(name);
}

inline TableRef Group::get_table(const char* name)
{
    return get_table_ptr(name)->get_table_ref();
}

inline ConstTableRef Group::get_table(const char* name) const
{
    return get_table_ptr(name)->get_table_ref();
}

template<class T> inline typename T::Ref Group::get_table(const char* name)
{
    return get_table_ptr<T>(name)->get_table_ref();
}

template<class T> inline typename T::ConstRef Group::get_table(const char* name) const
{
    return get_table_ptr<T>(name)->get_table_ref();
}

template<class S>
size_t Group::write(S& out)
{
    // Space for ref to top array
    out.write("\0\0\0\0\0\0\0\0", 8);

    // When serializing to disk we dont want
    // to include free space tracking as serialized
    // files are written without any free space.
    Array top(COLUMN_HASREFS, NULL, 0, m_alloc);
    top.add(m_top.Get(0));
    top.add(m_top.Get(1));

    // Recursively write all arrays
    const uint64_t topPos = top.Write(out);
    const size_t byte_size = out.getpos();

    // top ref
    out.seek(0);
    out.write((const char*)&topPos, 8);

    // Clean up temporary top
    top.Set(0, 0); // reset to avoid recursive delete
    top.Set(1, 0); // reset to avoid recursive delete
    top.Destroy();

    // return bytes written
    return byte_size;
}

template<class S>
void Group::to_json(S& out) const
{
    if (!m_top.IsValid()) {
        out << "{}";
        return;
    }

    out << "{";

    for (size_t i = 0; i < m_tables.Size(); ++i) {
        const char* const name = m_tableNames.Get(i);
        const Table* const table = get_table_ptr(i);

        if (i) out << ",";
        out << "\"" << name << "\"";
        out << ":";
        table->to_json(out);
    }

    out << "}";
}


} // namespace tightdb

#endif // TIGHTDB_GROUP_HPP
