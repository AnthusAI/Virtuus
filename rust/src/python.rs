use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyType};
use pyo3::PyCell;
use pythonize::{depythonize, pythonize};
use serde_json::Value;

use crate::gsi::Gsi;
use crate::sort::{OrderedValue, SortCondition};
use crate::table::{Association, ChangeSummary, Table, TableKey, ValidationMode};

type TableRef = Arc<Mutex<Table>>;

#[pyclass(name = "GSI")]
struct PyGsi {
    inner: Arc<Mutex<Gsi>>,
}

#[pymethods]
impl PyGsi {
    #[new]
    #[pyo3(signature = (name, partition_key, sort_key=None))]
    fn new(name: String, partition_key: String, sort_key: Option<String>) -> Self {
        let gsi = Gsi::new(&name, &partition_key, sort_key.as_deref());
        Self {
            inner: Arc::new(Mutex::new(gsi)),
        }
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        let gsi = self.inner.lock().expect("lock gsi");
        Ok(gsi.name().to_string())
    }

    #[getter]
    fn partition_key(&self) -> PyResult<String> {
        let gsi = self.inner.lock().expect("lock gsi");
        Ok(gsi.partition_key().to_string())
    }

    #[getter]
    fn sort_key(&self) -> PyResult<Option<String>> {
        let gsi = self.inner.lock().expect("lock gsi");
        Ok(gsi.sort_key().map(|value| value.to_string()))
    }

    fn put(&self, pk: String, record: &PyAny) -> PyResult<()> {
        let value = depythonize::<Value>(record)?;
        let mut gsi = self.inner.lock().expect("lock gsi");
        gsi.put(&pk, &value);
        Ok(())
    }

    fn remove(&self, pk: String, record: &PyAny) -> PyResult<()> {
        let value = depythonize::<Value>(record)?;
        let mut gsi = self.inner.lock().expect("lock gsi");
        gsi.remove(&pk, &value);
        Ok(())
    }

    fn update(&self, pk: String, old_record: &PyAny, new_record: &PyAny) -> PyResult<()> {
        let old_value = depythonize::<Value>(old_record)?;
        let new_value = depythonize::<Value>(new_record)?;
        let mut gsi = self.inner.lock().expect("lock gsi");
        gsi.update(&pk, &old_value, &new_value);
        Ok(())
    }

    #[pyo3(signature = (partition_value, sort_condition=None, sort_direction="asc"))]
    fn query(
        &self,
        py: Python<'_>,
        partition_value: &PyAny,
        sort_condition: Option<&PyAny>,
        sort_direction: &str,
    ) -> PyResult<Vec<String>> {
        if sort_direction != "asc" && sort_direction != "desc" {
            return Err(PyValueError::new_err(
                "sort_direction must be 'asc' or 'desc'",
            ));
        }
        let value = depythonize::<Value>(partition_value)?;
        let descending = sort_direction == "desc";
        let gsi = self.inner.lock().expect("lock gsi");
        if sort_condition.is_none() {
            return Ok(gsi.query(&value, None, descending));
        }
        let predicate = sort_condition.expect("checked");
        let mut entries = gsi.entries(&value);
        entries.retain(|(_, sort_value)| match sort_value {
            Some(v) => {
                let obj = pythonize(py, v).expect("pythonize");
                predicate.call1((obj,)).is_ok_and(|result| result.is_true().unwrap_or(false))
            }
            None => false,
        });
        if gsi.sort_key().is_some() {
            entries.sort_by(|a, b| {
                OrderedValue(a.1.clone().unwrap_or(Value::Null))
                    .cmp(&OrderedValue(b.1.clone().unwrap_or(Value::Null)))
            });
        }
        if descending {
            entries.reverse();
        }
        Ok(entries.into_iter().map(|entry| entry.0).collect())
    }
}

#[pyclass]
struct PyTableGsi {
    table: TableRef,
    name: String,
}

#[pymethods]
impl PyTableGsi {
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[getter]
    fn partition_key(&self) -> PyResult<String> {
        let table = self.table.lock().expect("lock table");
        let gsi = table
            .gsis()
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("GSI not found"))?;
        Ok(gsi.partition_key().to_string())
    }

    #[getter]
    fn sort_key(&self) -> PyResult<Option<String>> {
        let table = self.table.lock().expect("lock table");
        let gsi = table
            .gsis()
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("GSI not found"))?;
        Ok(gsi.sort_key().map(|value| value.to_string()))
    }

    fn put(&self, pk: String, record: &PyAny) -> PyResult<()> {
        let value = depythonize::<Value>(record)?;
        let mut table = self.table.lock().expect("lock table");
        let gsi = table
            .gsis_mut()
            .get_mut(&self.name)
            .ok_or_else(|| PyKeyError::new_err("GSI not found"))?;
        gsi.put(&pk, &value);
        Ok(())
    }

    fn remove(&self, pk: String, record: &PyAny) -> PyResult<()> {
        let value = depythonize::<Value>(record)?;
        let mut table = self.table.lock().expect("lock table");
        let gsi = table
            .gsis_mut()
            .get_mut(&self.name)
            .ok_or_else(|| PyKeyError::new_err("GSI not found"))?;
        gsi.remove(&pk, &value);
        Ok(())
    }

    fn update(&self, pk: String, old_record: &PyAny, new_record: &PyAny) -> PyResult<()> {
        let old_value = depythonize::<Value>(old_record)?;
        let new_value = depythonize::<Value>(new_record)?;
        let mut table = self.table.lock().expect("lock table");
        let gsi = table
            .gsis_mut()
            .get_mut(&self.name)
            .ok_or_else(|| PyKeyError::new_err("GSI not found"))?;
        gsi.update(&pk, &old_value, &new_value);
        Ok(())
    }

    #[pyo3(signature = (partition_value, sort_condition=None, sort_direction="asc"))]
    fn query(
        &self,
        py: Python<'_>,
        partition_value: &PyAny,
        sort_condition: Option<&PyAny>,
        sort_direction: &str,
    ) -> PyResult<Vec<String>> {
        if sort_direction != "asc" && sort_direction != "desc" {
            return Err(PyValueError::new_err(
                "sort_direction must be 'asc' or 'desc'",
            ));
        }
        let value = depythonize::<Value>(partition_value)?;
        let descending = sort_direction == "desc";
        let table = self.table.lock().expect("lock table");
        let gsi = table
            .gsis()
            .get(&self.name)
            .ok_or_else(|| PyKeyError::new_err("GSI not found"))?;
        if sort_condition.is_none() {
            return Ok(gsi.query(&value, None, descending));
        }
        let predicate = sort_condition.expect("checked");
        let mut entries = gsi.entries(&value);
        entries.retain(|(_, sort_value)| match sort_value {
            Some(v) => {
                let obj = pythonize(py, v).expect("pythonize");
                predicate.call1((obj,)).is_ok_and(|result| result.is_true().unwrap_or(false))
            }
            None => false,
        });
        if gsi.sort_key().is_some() {
            entries.sort_by(|a, b| {
                OrderedValue(a.1.clone().unwrap_or(Value::Null))
                    .cmp(&OrderedValue(b.1.clone().unwrap_or(Value::Null)))
            });
        }
        if descending {
            entries.reverse();
        }
        Ok(entries.into_iter().map(|entry| entry.0).collect())
    }
}

#[pyclass]
struct PyGsiMap {
    table: TableRef,
}

#[pymethods]
impl PyGsiMap {
    fn __contains__(&self, name: &str) -> PyResult<bool> {
        let table = self.table.lock().expect("lock table");
        Ok(table.gsis().contains_key(name))
    }

    fn __getitem__(&self, name: &str) -> PyResult<PyTableGsi> {
        let table = self.table.lock().expect("lock table");
        if !table.gsis().contains_key(name) {
            return Err(PyKeyError::new_err(name.to_string()));
        }
        Ok(PyTableGsi {
            table: Arc::clone(&self.table),
            name: name.to_string(),
        })
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        let table = self.table.lock().expect("lock table");
        Ok(table.gsis().keys().cloned().collect())
    }

    fn values(&self) -> PyResult<Vec<PyTableGsi>> {
        let table = self.table.lock().expect("lock table");
        Ok(table
            .gsis()
            .keys()
            .map(|name| PyTableGsi {
                table: Arc::clone(&self.table),
                name: name.clone(),
            })
            .collect())
    }

    fn items(&self) -> PyResult<Vec<(String, PyTableGsi)>> {
        let table = self.table.lock().expect("lock table");
        Ok(table
            .gsis()
            .keys()
            .map(|name| {
                (
                    name.clone(),
                    PyTableGsi {
                        table: Arc::clone(&self.table),
                        name: name.clone(),
                    },
                )
            })
            .collect())
    }

    #[pyo3(signature = (name, default=None))]
    fn pop(&self, name: &str, default: Option<PyObject>, py: Python<'_>) -> PyResult<PyObject> {
        let mut table = self.table.lock().expect("lock table");
        let removed = table.remove_gsi(name);
        if removed.is_some() {
            return Ok(py.None());
        }
        Ok(default.unwrap_or_else(|| py.None()))
    }
}

#[pyclass(name = "Table")]
struct PyTable {
    inner: TableRef,
    on_put: Py<PyList>,
    on_delete: Py<PyList>,
    on_refresh: Py<PyList>,
    hook_errors: Arc<Mutex<Vec<String>>>,
}

#[pymethods]
impl PyTable {
    #[new]
    #[pyo3(
        signature = (
            name,
            primary_key=None,
            partition_key=None,
            sort_key=None,
            directory=None,
            validation="silent",
            check_interval=0,
            auto_refresh=true
        )
    )]
    fn new(
        py: Python<'_>,
        name: String,
        primary_key: Option<String>,
        partition_key: Option<String>,
        sort_key: Option<String>,
        directory: Option<String>,
        validation: &str,
        check_interval: u64,
        auto_refresh: bool,
    ) -> PyResult<Self> {
        let validation = parse_validation(validation)?;
        let table = Table::new(
            &name,
            primary_key.as_deref(),
            partition_key.as_deref(),
            sort_key.as_deref(),
            directory.map(PathBuf::from),
            validation,
        );
        let table = Arc::new(Mutex::new(table));
        {
            let mut table = table.lock().expect("lock table");
            table.set_check_interval(check_interval);
            table.set_auto_refresh(auto_refresh);
        }
        Ok(Self {
            inner: table,
            on_put: PyList::empty(py).into(),
            on_delete: PyList::empty(py).into(),
            on_refresh: PyList::empty(py).into(),
            hook_errors: Arc::new(Mutex::new(Vec::new())),
        })
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.name().to_string())
    }

    #[getter]
    fn primary_key(&self) -> PyResult<Option<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.primary_key().map(|value| value.to_string()))
    }

    #[getter]
    fn partition_key(&self) -> PyResult<Option<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.partition_key().map(|value| value.to_string()))
    }

    #[getter]
    fn sort_key(&self) -> PyResult<Option<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.sort_key().map(|value| value.to_string()))
    }

    #[getter]
    fn directory(&self) -> PyResult<Option<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.directory().map(|path| path.to_string_lossy().to_string()))
    }

    #[getter]
    fn warnings(&self) -> PyResult<Vec<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.warnings().clone())
    }

    #[getter]
    fn hook_errors(&self) -> PyResult<Vec<String>> {
        let errors = self.hook_errors.lock().expect("lock errors");
        Ok(errors.clone())
    }

    #[getter]
    fn on_put(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.on_put.clone_ref(py).into_py(py))
    }

    #[getter]
    fn on_delete(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.on_delete.clone_ref(py).into_py(py))
    }

    #[getter]
    fn on_refresh(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.on_refresh.clone_ref(py).into_py(py))
    }

    #[getter]
    fn associations(&self) -> PyResult<Vec<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.associations().clone())
    }

    #[getter]
    fn association_defs(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        let defs = table.association_defs();
        let map = PyDict::new(py);
        for (name, assoc) in defs {
            let assoc_map = PyDict::new(py);
            match assoc {
                Association::BelongsTo {
                    target_table,
                    foreign_key,
                } => {
                    assoc_map.set_item("kind", "belongs_to")?;
                    assoc_map.set_item("target_table", target_table)?;
                    assoc_map.set_item("foreign_key", foreign_key)?;
                }
                Association::HasMany {
                    target_table,
                    index,
                } => {
                    assoc_map.set_item("kind", "has_many")?;
                    assoc_map.set_item("target_table", target_table)?;
                    assoc_map.set_item("index", index)?;
                }
                Association::HasManyThrough {
                    through_table,
                    through_index,
                    target_table,
                    target_foreign_key,
                } => {
                    assoc_map.set_item("kind", "has_many_through")?;
                    assoc_map.set_item("through_table", through_table)?;
                    assoc_map.set_item("through_index", through_index)?;
                    assoc_map.set_item("target_table", target_table)?;
                    assoc_map.set_item("target_foreign_key", target_foreign_key)?;
                }
            }
            map.set_item(name, assoc_map)?;
        }
        Ok(map.into())
    }

    #[getter]
    fn last_write_used_atomic(&self) -> PyResult<bool> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.last_write_used_atomic())
    }

    #[getter]
    fn last_change_summary(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        Ok(change_summary_to_py(py, &table.last_change_summary))
    }

    #[getter]
    fn refresh_errors(&self) -> PyResult<Vec<String>> {
        let table = self.inner.lock().expect("lock table");
        Ok(table.refresh_errors().to_vec())
    }

    #[getter]
    fn records(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        let dict = PyDict::new(py);
        for (key, record) in table.records() {
            let key_str = match key {
                TableKey::Simple(pk) => pk.clone(),
                TableKey::Composite(partition, sort) => format!("{partition}__{sort}"),
            };
            dict.set_item(key_str, pythonize(py, record)?)?;
        }
        Ok(dict.into())
    }

    #[getter]
    fn gsis(&self) -> PyResult<PyGsiMap> {
        Ok(PyGsiMap {
            table: Arc::clone(&self.inner),
        })
    }

    fn add_gsi(&self, name: String, partition_key: String, sort_key: Option<String>) {
        let mut table = self.inner.lock().expect("lock table");
        table.add_gsi(&name, &partition_key, sort_key.as_deref());
    }

    fn add_belongs_to(&self, name: String, target_table: String, foreign_key: String) {
        let mut table = self.inner.lock().expect("lock table");
        table.add_belongs_to(&name, &target_table, &foreign_key);
    }

    fn add_has_many(&self, name: String, target_table: String, index: String) {
        let mut table = self.inner.lock().expect("lock table");
        table.add_has_many(&name, &target_table, &index);
    }

    fn add_has_many_through(
        &self,
        name: String,
        through_table: String,
        through_index: String,
        target_table: String,
        target_foreign_key: String,
    ) {
        let mut table = self.inner.lock().expect("lock table");
        table.add_has_many_through(
            &name,
            &through_table,
            &through_index,
            &target_table,
            &target_foreign_key,
        );
    }

    fn put(&self, py: Python<'_>, record: &PyAny) -> PyResult<()> {
        let value = depythonize::<Value>(record)?;
        {
            let mut table = self.inner.lock().expect("lock table");
            table.put(value.clone());
        }
        self.fire_hooks(py, &self.on_put, &value);
        Ok(())
    }

    #[pyo3(signature = (pk, sort=None))]
    fn get(&self, py: Python<'_>, pk: String, sort: Option<String>) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        let result = table.get(&pk, sort.as_deref());
        match result {
            Some(value) => Ok(pythonize(py, &value)?),
            None => Ok(py.None()),
        }
    }

    #[pyo3(signature = (pk, sort=None))]
    fn delete(&self, py: Python<'_>, pk: String, sort: Option<String>) -> PyResult<()> {
        let record = {
            let table = self.inner.lock().expect("lock table");
            table.get(&pk, sort.as_deref())
        };
        {
            let mut table = self.inner.lock().expect("lock table");
            table.delete(&pk, sort.as_deref());
        }
        if let Some(record) = record {
            self.fire_hooks(py, &self.on_delete, &record);
        }
        Ok(())
    }

    fn scan(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let mut table = self.inner.lock().expect("lock table");
        let records = table.scan();
        records
            .iter()
            .map(|record| pythonize(py, record))
            .collect()
    }

    fn bulk_load(&self, records: &PyAny) -> PyResult<()> {
        let values = depythonize::<Vec<Value>>(records)?;
        let mut table = self.inner.lock().expect("lock table");
        table.bulk_load(values);
        Ok(())
    }

    #[pyo3(signature = (index=None, value=None))]
    fn count(&self, index: Option<String>, value: Option<&PyAny>) -> PyResult<usize> {
        let value = match value {
            Some(v) => Some(depythonize::<Value>(v)?),
            None => None,
        };
        let table = self.inner.lock().expect("lock table");
        Ok(table.count(index.as_deref(), value.as_ref()))
    }

    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        Ok(pythonize(py, &table.describe())?)
    }

    #[pyo3(signature = (name, partition_value, sort_condition=None, descending=false))]
    fn query_gsi(
        &self,
        py: Python<'_>,
        name: String,
        partition_value: &PyAny,
        sort_condition: Option<&PyAny>,
        descending: bool,
    ) -> PyResult<Vec<PyObject>> {
        let partition_value = depythonize::<Value>(partition_value)?;
        let mut table = self.inner.lock().expect("lock table");
        let sort_key = table
            .gsis()
            .get(&name)
            .ok_or_else(|| PyKeyError::new_err(format!("GSI {name} does not exist")))?
            .sort_key()
            .map(|value| value.to_string());
        let mut result = Vec::new();
        let mut records = table.query_gsi(&name, &partition_value, None, descending);
        if let Some(predicate) = sort_condition {
            if let Some(sort_key) = sort_key.as_deref() {
                records.retain(|record| match record.get(sort_key) {
                    Some(value) => {
                        let obj = pythonize(py, value).expect("pythonize");
                        predicate
                            .call1((obj,))
                            .is_ok_and(|res| res.is_true().unwrap_or(false))
                    }
                    None => false,
                });
            } else {
                records.clear();
            }
        }
        for record in records {
            result.push(pythonize(py, &record)?);
        }
        Ok(result)
    }

    fn load_from_dir(&self, directory: Option<String>) -> PyResult<()> {
        let mut table = self.inner.lock().expect("lock table");
        let directory = directory.map(PathBuf::from);
        table.load_from_dir(directory);
        Ok(())
    }

    fn export(&self, directory: String) -> PyResult<()> {
        let table = self.inner.lock().expect("lock table");
        table.export(PathBuf::from(directory));
        Ok(())
    }

    #[pyo3(signature = (force_scan=false))]
    fn is_stale(&self, force_scan: bool) -> PyResult<bool> {
        let mut table = self.inner.lock().expect("lock table");
        Ok(table.is_stale(force_scan))
    }

    fn check(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        Ok(change_summary_to_py(py, &table.check()))
    }

    fn refresh(&self, py: Python<'_>) -> PyResult<PyObject> {
        let mut table = self.inner.lock().expect("lock table");
        let summary = table.refresh();
        let summary_value = change_summary_to_value(&summary);
        drop(table);
        self.fire_hooks(py, &self.on_refresh, &summary_value);
        Ok(change_summary_to_py(py, &summary))
    }

    fn warm(&self) -> PyResult<()> {
        let mut table = self.inner.lock().expect("lock table");
        table.warm();
        Ok(())
    }

    fn resolve_association(
        &self,
        py: Python<'_>,
        name: String,
        pk: String,
        tables: &PyAny,
    ) -> PyResult<PyObject> {
        let table = self.inner.lock().expect("lock table");
        let definition = table.association(name.as_str()).cloned();
        let record = table.get(&pk, None);
        drop(table);
        let definition = match definition {
            Some(def) => def,
            None => return Err(PyKeyError::new_err(format!("association {name} not defined"))),
        };
        let record = match record {
            Some(record) => record,
            None => return Ok(py.None()),
        };
        let result = resolve_association_py(py, name.as_str(), definition, record, tables)?;
        Ok(result)
    }
}

#[pyclass]
struct PyTableMap {
    tables: Arc<Mutex<HashMap<String, TableRef>>>,
}

#[pymethods]
impl PyTableMap {
    fn __contains__(&self, name: &str) -> PyResult<bool> {
        let tables = self.tables.lock().expect("lock tables");
        Ok(tables.contains_key(name))
    }

    fn __getitem__(&self, name: &str) -> PyResult<PyTable> {
        let tables = self.tables.lock().expect("lock tables");
        let table = tables
            .get(name)
            .ok_or_else(|| PyKeyError::new_err(name.to_string()))?;
        Ok(PyTable {
            inner: Arc::clone(table),
            on_put: Python::with_gil(|py| PyList::empty(py).into()),
            on_delete: Python::with_gil(|py| PyList::empty(py).into()),
            on_refresh: Python::with_gil(|py| PyList::empty(py).into()),
            hook_errors: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        let tables = self.tables.lock().expect("lock tables");
        Ok(tables.keys().cloned().collect())
    }

    fn values(&self) -> PyResult<Vec<PyTable>> {
        let tables = self.tables.lock().expect("lock tables");
        Ok(tables
            .values()
            .map(|table| PyTable {
                inner: Arc::clone(table),
                on_put: Python::with_gil(|py| PyList::empty(py).into()),
                on_delete: Python::with_gil(|py| PyList::empty(py).into()),
                on_refresh: Python::with_gil(|py| PyList::empty(py).into()),
                hook_errors: Arc::new(Mutex::new(Vec::new())),
            })
            .collect())
    }

    fn items(&self) -> PyResult<Vec<(String, PyTable)>> {
        let tables = self.tables.lock().expect("lock tables");
        Ok(tables
            .iter()
            .map(|(name, table)| {
                (
                    name.clone(),
                    PyTable {
                        inner: Arc::clone(table),
                        on_put: Python::with_gil(|py| PyList::empty(py).into()),
                        on_delete: Python::with_gil(|py| PyList::empty(py).into()),
                        on_refresh: Python::with_gil(|py| PyList::empty(py).into()),
                        hook_errors: Arc::new(Mutex::new(Vec::new())),
                    },
                )
            })
            .collect())
    }
}

#[pyclass(name = "Database")]
struct PyDatabase {
    tables: Arc<Mutex<HashMap<String, TableRef>>>,
}

#[pymethods]
impl PyDatabase {
    #[new]
    fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn add_table(&self, name: String, table: PyRef<'_, PyTable>) {
        let mut tables = self.tables.lock().expect("lock tables");
        tables.insert(name, Arc::clone(&table.inner));
    }

    #[getter]
    fn tables(&self) -> PyResult<PyTableMap> {
        Ok(PyTableMap {
            tables: Arc::clone(&self.tables),
        })
    }

    fn warm(&self) {
        let tables = self.tables.lock().expect("lock tables");
        for table in tables.values() {
            let mut table = table.lock().expect("lock table");
            table.warm();
        }
    }

    fn check(&self, py: Python<'_>) -> PyResult<PyObject> {
        let tables = self.tables.lock().expect("lock tables");
        let map = PyDict::new(py);
        for (name, table) in tables.iter() {
            let table = table.lock().expect("lock table");
            map.set_item(name, change_summary_to_py(py, &table.check()))?;
        }
        Ok(map.into())
    }

    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        let tables = self.tables.lock().expect("lock tables");
        let map = PyDict::new(py);
        for (name, table) in tables.iter() {
            let table = table.lock().expect("lock table");
            let mut description = table.describe();
            description
                .as_object_mut()
                .expect("description map")
                .insert("stale".to_string(), Value::Bool(table.is_stale(false)));
            map.set_item(name, pythonize(py, &description)?)?;
        }
        Ok(map.into())
    }

    fn validate(&self, py: Python<'_>) -> PyResult<PyObject> {
        let mut violations: Vec<Value> = Vec::new();
        let tables = self.tables.lock().expect("lock tables");
        for (table_name, table_ref) in tables.iter() {
            let mut table = table_ref.lock().expect("lock table");
            for (assoc_name, definition) in table.association_defs() {
                if let Association::BelongsTo {
                    target_table,
                    foreign_key,
                } = definition
                {
                    for record in table.scan() {
                        let fk_value = match record.get(foreign_key) {
                            Some(value) => value.clone(),
                            None => continue,
                        };
                        let target = tables
                            .get(target_table)
                            .and_then(|target| {
                                let target = target.lock().expect("lock table");
                                let fk_str = fk_value.as_str().unwrap_or(&fk_value.to_string());
                                target.get(fk_str, None)
                            });
                        if target.is_none() {
                            violations.push(serde_json::json!({
                                "table": table_name,
                                "record_pk": record.get(table.key_field().unwrap_or("id")).cloned().unwrap_or(Value::Null),
                                "association": assoc_name,
                                "foreign_key": foreign_key,
                                "missing_target": fk_value,
                            }));
                        }
                    }
                }
            }
        }
        Ok(pythonize(py, &violations)?)
    }

    fn execute(&self, py: Python<'_>, query: &PyAny) -> PyResult<PyObject> {
        let query_value = depythonize::<Value>(query)?;
        let (table_name, directive) = parse_query(&query_value)?;
        let table_ref = {
            let tables = self.tables.lock().expect("lock tables");
            tables
                .get(&table_name)
                .cloned()
                .ok_or_else(|| {
                    PyKeyError::new_err(format!("table \"{table_name}\" does not exist"))
                })?
        };
        let directive = directive.as_object().cloned().unwrap_or_default();
        let mut table = table_ref.lock().expect("lock table");
        if let Some(pk_value) = directive.get("pk") {
            let pk = pk_value.as_str().unwrap_or(&pk_value.to_string()).to_string();
            let sort = directive.get("sort").and_then(|value| value.as_str()).map(|s| s.to_string());
            let mut result = table.get(&pk, sort.as_deref()).unwrap_or(Value::Null);
            if let Some(fields) = directive.get("fields").and_then(|v| v.as_array()) {
                result = project(&result, fields);
            }
            let includes = directive.get("include").and_then(|v| v.as_object()).cloned();
            drop(table);
            let enriched = apply_includes(self, &table_name, result, includes.as_ref());
            return Ok(pythonize(py, &enriched)?);
        }

        let items: Vec<Value> = if let Some(index_value) = directive.get("index") {
            let gsi_name = index_value.as_str().unwrap_or_default();
            let where_map = directive
                .get("where")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let gsi = table
                .gsis()
                .get(gsi_name)
                .ok_or_else(|| PyKeyError::new_err(format!("GSI \"{gsi_name}\" does not exist")))?;
            let partition_field = gsi.partition_key().to_string();
            let partition_value = where_map.get(&partition_field).cloned().unwrap_or(Value::Null);
            let sort_condition = directive
                .get("sort")
                .and_then(build_sort_condition);
            let descending = directive
                .get("sort_direction")
                .and_then(|v| v.as_str())
                .map(|s| s == "desc")
                .unwrap_or(false);
            table.query_gsi(gsi_name, &partition_value, sort_condition.as_ref(), descending)
        } else {
            let where_map = directive
                .get("where")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            table
                .scan()
                .into_iter()
                .filter(|record| record_matches(record, &where_map))
                .collect()
        };

        let mut projected: Vec<Value> = items
            .into_iter()
            .map(|record| {
                if let Some(fields) = directive.get("fields").and_then(|v| v.as_array()) {
                    project(&record, fields)
                } else {
                    record
                }
            })
            .collect();
        let start = directive
            .get("next_token")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let limit = directive
            .get("limit")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize);
        let mut next_token: Option<String> = None;
        if let Some(limit) = limit {
            let end = start + limit;
            if end < projected.len() {
                next_token = Some(end.to_string());
            }
            projected = projected.into_iter().skip(start).take(limit).collect();
        }
        let mut result = serde_json::json!({"items": projected});
        if let Some(token) = next_token {
            result
                .as_object_mut()
                .expect("result map")
                .insert("next_token".to_string(), Value::String(token));
        }
        if let Some(include_map) = directive.get("include").and_then(|v| v.as_object()).cloned()
        {
            let items = result.get_mut("items").and_then(|v| v.as_array_mut());
            if let Some(items) = items {
                for item in items.iter_mut() {
                    *item = apply_includes(self, &table_name, item.clone(), Some(&include_map));
                }
            }
        }
        Ok(pythonize(py, &result)?)
    }

    #[classmethod]
    fn from_schema(_cls: &PyType, py: Python<'_>, path: String, data_root: Option<String>) -> PyResult<Py<PyDatabase>> {
        let content = std::fs::read_to_string(&path)
            .map_err(|err| PyValueError::new_err(format!("failed to read schema: {err}")))?;
        let schema: serde_yaml::Value = serde_yaml::from_str(&content)
            .map_err(|err| PyValueError::new_err(format!("failed to parse schema: {err}")))?;
        let schema_json = serde_json::to_value(schema).unwrap_or(Value::Null);
        let mut db = PyDatabase::new();
        let tables_conf = schema_json
            .get("tables")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        for (name, conf) in tables_conf {
            let primary_key = conf.get("primary_key").and_then(|v| v.as_str());
            let partition_key = conf.get("partition_key").and_then(|v| v.as_str());
            let sort_key = conf.get("sort_key").and_then(|v| v.as_str());
            let mut directory = conf.get("directory").and_then(|v| v.as_str()).map(|s| s.to_string());
            if let (Some(root), Some(dir)) = (data_root.as_ref(), directory.as_ref()) {
                directory = Some(PathBuf::from(root).join(dir).to_string_lossy().to_string());
            }
            let table = Table::new(
                &name,
                primary_key,
                partition_key,
                sort_key,
                directory.clone().map(PathBuf::from),
                ValidationMode::Warn,
            );
            let table_ref = Arc::new(Mutex::new(table));
            if let Some(gsis) = conf.get("gsis").and_then(|v| v.as_object()) {
                for (gsi_name, gsi_conf) in gsis {
                    let partition_key = gsi_conf
                        .get("partition_key")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let sort_key = gsi_conf.get("sort_key").and_then(|v| v.as_str());
                    let mut table = table_ref.lock().expect("lock table");
                    table.add_gsi(gsi_name, partition_key, sort_key);
                }
            }
            if let Some(assocs) = conf.get("associations").and_then(|v| v.as_object()) {
                for (assoc_name, assoc_conf) in assocs {
                    let kind = assoc_conf.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    let mut table = table_ref.lock().expect("lock table");
                    match kind {
                        "belongs_to" => {
                            let target_table = assoc_conf.get("table").and_then(|v| v.as_str()).unwrap_or("");
                            let foreign_key = assoc_conf.get("foreign_key").and_then(|v| v.as_str()).unwrap_or("");
                            table.add_belongs_to(assoc_name, target_table, foreign_key);
                        }
                        "has_many" => {
                            let target_table = assoc_conf.get("table").and_then(|v| v.as_str()).unwrap_or("");
                            let index = assoc_conf.get("index").and_then(|v| v.as_str()).unwrap_or("");
                            table.add_has_many(assoc_name, target_table, index);
                        }
                        "has_many_through" => {
                            let through_table = assoc_conf.get("through").and_then(|v| v.as_str()).unwrap_or("");
                            let index = assoc_conf.get("index").and_then(|v| v.as_str()).unwrap_or("");
                            let target_table = assoc_conf.get("table").and_then(|v| v.as_str()).unwrap_or("");
                            let foreign_key = assoc_conf.get("foreign_key").and_then(|v| v.as_str()).unwrap_or("");
                            table.add_has_many_through(assoc_name, through_table, index, target_table, foreign_key);
                        }
                        _ => {}
                    }
                }
            }
            db.tables
                .lock()
                .expect("lock tables")
                .insert(name, Arc::clone(&table_ref));
            if directory.is_some() {
                let mut table = table_ref.lock().expect("lock table");
                table.load_from_dir(None);
            }
        }
        Py::new(py, db)
    }
}

fn parse_validation(value: &str) -> PyResult<ValidationMode> {
    match value {
        "silent" => Ok(ValidationMode::Silent),
        "warn" => Ok(ValidationMode::Warn),
        "error" => Ok(ValidationMode::Error),
        _ => Err(PyValueError::new_err(
            "validation must be silent, warn, or error",
        )),
    }
}

fn change_summary_to_value(summary: &ChangeSummary) -> Value {
    serde_json::json!({
        "added": summary.added,
        "modified": summary.modified,
        "deleted": summary.deleted,
        "reread": summary.reread,
    })
}

fn change_summary_to_py(py: Python<'_>, summary: &ChangeSummary) -> PyObject {
    pythonize(py, &change_summary_to_value(summary)).expect("pythonize")
}

fn parse_query(query: &Value) -> PyResult<(String, Value)> {
    let map = query
        .as_object()
        .ok_or_else(|| PyValueError::new_err("query must be a mapping"))?;
    if map.len() != 1 {
        return Err(PyValueError::new_err(
            "query must target exactly one table",
        ));
    }
    let (table_name, directive) = map.iter().next().expect("checked");
    Ok((table_name.clone(), directive.clone()))
}

fn record_matches(record: &Value, where_map: &serde_json::Map<String, Value>) -> bool {
    for (key, expected) in where_map {
        if record.get(key) != Some(expected) {
            return false;
        }
    }
    true
}

fn project(record: &Value, fields: &[Value]) -> Value {
    if !record.is_object() || fields.is_empty() {
        return record.clone();
    }
    let mut obj = serde_json::Map::new();
    for field in fields {
        if let Some(name) = field.as_str() {
            if let Some(value) = record.get(name) {
                obj.insert(name.to_string(), value.clone());
            }
        }
    }
    Value::Object(obj)
}

fn build_sort_condition(value: &Value) -> Option<SortCondition> {
    let map = value.as_object()?;
    let (op, operand) = map.iter().next()?;
    match op.as_str() {
        "eq" => Some(SortCondition::Eq(operand.clone())),
        "ne" => Some(SortCondition::Ne(operand.clone())),
        "lt" => Some(SortCondition::Lt(operand.clone())),
        "lte" => Some(SortCondition::Lte(operand.clone())),
        "gt" => Some(SortCondition::Gt(operand.clone())),
        "gte" => Some(SortCondition::Gte(operand.clone())),
        "between" => operand.as_array().and_then(|a| {
            if a.len() == 2 {
                Some(SortCondition::Between(a[0].clone(), a[1].clone()))
            } else {
                None
            }
        }),
        "begins_with" => operand
            .as_str()
            .map(|s| SortCondition::BeginsWith(s.to_string())),
        "contains" => operand
            .as_str()
            .map(|s| SortCondition::Contains(s.to_string())),
        _ => None,
    }
}

fn resolve_association_py(
    py: Python<'_>,
    assoc_name: &str,
    definition: Association,
    record: Value,
    tables: &PyAny,
) -> PyResult<PyObject> {
    let tables_dict = tables.downcast::<PyDict>()?;
    match definition {
        Association::BelongsTo {
            target_table,
            foreign_key,
        } => {
            let fk_value = record.get(&foreign_key).cloned().unwrap_or(Value::Null);
            if fk_value.is_null() {
                return Ok(py.None());
            }
            let table_obj = tables_dict
                .get_item(target_table)
                .ok_or_else(|| PyKeyError::new_err("target table not found"))?;
            let table = table_obj.downcast::<PyCell<PyTable>>()?;
            let fk_str = fk_value.as_str().unwrap_or(&fk_value.to_string());
            table.borrow().get(py, fk_str.to_string(), None)
        }
        Association::HasMany { target_table, index } => {
            let key_field = record
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let table_obj = tables_dict
                .get_item(target_table)
                .ok_or_else(|| PyKeyError::new_err("target table not found"))?;
            let table = table_obj.downcast::<PyCell<PyTable>>()?;
            let key_obj = pythonize(py, &Value::String(key_field))?;
            let records = table
                .borrow()
                .query_gsi(py, index, key_obj.as_ref(py), None, false)?;
            Ok(PyList::new(py, records).into())
        }
        Association::HasManyThrough {
            through_table,
            through_index,
            target_table,
            target_foreign_key,
        } => {
            let key_value = record
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let through_obj = tables_dict
                .get_item(through_table)
                .ok_or_else(|| PyKeyError::new_err("through table not found"))?;
            let through = through_obj.downcast::<PyCell<PyTable>>()?;
            let key_obj = pythonize(py, &Value::String(key_value))?;
            let junctions = through
                .borrow()
                .query_gsi(py, through_index, key_obj.as_ref(py), None, false)?;
            let target_obj = tables_dict
                .get_item(target_table)
                .ok_or_else(|| PyKeyError::new_err("target table not found"))?;
            let target = target_obj.downcast::<PyCell<PyTable>>()?;
            let mut results = Vec::new();
            for junction in junctions {
                let junction_value = depythonize::<Value>(junction.as_ref(py))?;
                if let Some(fk_value) = junction_value.get(&target_foreign_key) {
                    let fk_str = fk_value.as_str().unwrap_or(&fk_value.to_string()).to_string();
                    let record = target.borrow().get(py, fk_str, None)?;
                    if !record.is_none(py) {
                        results.push(record);
                    }
                }
            }
            Ok(PyList::new(py, results).into())
        }
    }
}

fn apply_includes(
    db: &PyDatabase,
    table_name: &str,
    record: Value,
    includes: Option<&serde_json::Map<String, Value>>,
) -> Value {
    let Some(include_map) = includes else {
        return record;
    };
    if record.is_null() {
        return record;
    }
    let tables = db.tables.lock().expect("lock tables");
    let table = match tables.get(table_name) {
        Some(table) => table.lock().expect("lock table"),
        None => return record,
    };
    let association_defs = table.association_defs().clone();
    let key_field = table.key_field().unwrap_or("id").to_string();
    drop(table);
    let pk = record
        .get(&key_field)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let mut enriched = record;
    for (assoc_name, assoc_directive) in include_map {
        let definition = match association_defs.get(assoc_name) {
            Some(def) => def.clone(),
            None => continue,
        };
        let related = resolve_association_value(&tables, table_name, assoc_name, &pk, definition);
        let target_table = association_defs
            .get(assoc_name)
            .map(|d| match d {
                Association::BelongsTo { target_table, .. } => target_table.clone(),
                Association::HasMany { target_table, .. } => target_table.clone(),
                Association::HasManyThrough { target_table, .. } => target_table.clone(),
            })
            .unwrap_or_else(|| table_name.to_string());
        if related.is_null() {
            enriched[assoc_name] = Value::Null;
            continue;
        }
        if let Some(array) = related.as_array() {
            let mut items = Vec::new();
            for mut item in array.clone() {
                if let Some(fields) = assoc_directive.get("fields").and_then(|v| v.as_array()) {
                    item = project(&item, fields);
                }
                if let Some(nested) = assoc_directive.get("include").and_then(|v| v.as_object()) {
                    item = apply_includes(db, &target_table, item, Some(nested));
                }
                items.push(item);
            }
            enriched[assoc_name] = Value::Array(items);
        } else {
            let mut item = related;
            if let Some(fields) = assoc_directive.get("fields").and_then(|v| v.as_array()) {
                item = project(&item, fields);
            }
            if let Some(nested) = assoc_directive.get("include").and_then(|v| v.as_object()) {
                item = apply_includes(db, &target_table, item, Some(nested));
            }
            enriched[assoc_name] = item;
        }
    }
    enriched
}

fn resolve_association_value(
    tables: &HashMap<String, TableRef>,
    table_name: &str,
    assoc_name: &str,
    pk: &str,
    definition: Association,
) -> Value {
    let table = match tables.get(table_name) {
        Some(table) => table.lock().expect("lock table"),
        None => return Value::Null,
    };
    let record = table.get(pk, None);
    drop(table);
    let record = match record {
        Some(record) => record,
        None => return Value::Null,
    };
    match definition {
        Association::BelongsTo {
            target_table,
            foreign_key,
        } => {
            let fk_value = record.get(&foreign_key).cloned().unwrap_or(Value::Null);
            if fk_value.is_null() {
                return Value::Null;
            }
            let target = tables.get(&target_table);
            if let Some(target) = target {
                let target = target.lock().expect("lock table");
                let fk_str = fk_value.as_str().unwrap_or(&fk_value.to_string());
                target.get(fk_str, None).unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        Association::HasMany { target_table, index } => {
            let key_field = record
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let target = tables.get(&target_table);
            if let Some(target) = target {
                let mut target = target.lock().expect("lock table");
                let results = target.query_gsi(&index, &Value::String(key_field), None, false);
                Value::Array(results)
            } else {
                Value::Array(Vec::new())
            }
        }
        Association::HasManyThrough {
            through_table,
            through_index,
            target_table,
            target_foreign_key,
        } => {
            let key_value = record
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let through = tables.get(&through_table);
            let target = tables.get(&target_table);
            if through.is_none() || target.is_none() {
                return Value::Array(Vec::new());
            }
            let mut through = through.unwrap().lock().expect("lock table");
            let junctions =
                through.query_gsi(&through_index, &Value::String(key_value), None, false);
            let mut target = target.unwrap().lock().expect("lock table");
            let mut results = Vec::new();
            for junction in junctions {
                if let Some(fk_value) = junction.get(&target_foreign_key) {
                    let fk_str = fk_value.as_str().unwrap_or(&fk_value.to_string());
                    if let Some(record) = target.get(fk_str, None) {
                        results.push(record);
                    }
                }
            }
            Value::Array(results)
        }
    }
}

impl PyTable {
    fn fire_hooks(&self, py: Python<'_>, hooks: &Py<PyList>, payload: &Value) {
        let list = hooks.borrow(py);
        for hook in list.iter() {
            let record = pythonize(py, payload).expect("pythonize");
            if hook.call1((record,)).is_err() {
                let mut errors = self.hook_errors.lock().expect("lock errors");
                errors.push("hook error".to_string());
            }
        }
    }
}

#[pymodule]
fn _rust(py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_class::<PyTable>()?;
    module.add_class::<PyDatabase>()?;
    module.add_class::<PyGsi>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add("BACKEND", "rust")?;
    let _ = py;
    Ok(())
}
