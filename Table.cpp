#include "Table.h"
#include <assert.h>

Table::Table(const char* name) : m_name(name), m_size(0) {
}

Table::~Table() {
	for (size_t i = 0; i < m_columns.Size(); ++i) {
		Column* column = (Column*)m_columns.Get(i);
		delete column;
	}
}

void Table::RegisterColumn(const char* name) {
	Column* newColumn = new Column();
	
	m_columnNames.Add((int)name);
	m_columns.Add((int)newColumn);
}

size_t Table::AddRow() {
	const size_t len = m_columns.Size();
	for (size_t i = 0; i < len; ++i) {
		Column* column = (Column*)m_columns.Get(i);
		column->Add(0);
	}

	return m_size++;
}

void Table::Clear() {
	for (size_t i = 0; i < m_columns.Size(); ++i) {
		Column* column = (Column*)m_columns.Get(i);
		column->Clear();
	}
	m_size = 0;
}

void Table::DeleteRow(size_t ndx) {
	assert(ndx < m_size);

	for (size_t i = 0; i < m_columns.Size(); ++i) {
		Column* column = (Column*)m_columns.Get(i);
		column->Delete(ndx);
	}
}

int Table::Get(size_t column_id, size_t ndx) const {
	assert(column_id < m_columns.Size());
	assert(ndx < m_size);

	const Column* const column = (const Column*)m_columns.Get(column_id);
	return column->Get(ndx);
}

void Table::Set(size_t column_id, size_t ndx, int value) {
	assert(column_id < m_columns.Size());
	assert(ndx < m_size);

	Column* const column = (Column*)m_columns.Get(column_id);
	column->Set(ndx, value);
}