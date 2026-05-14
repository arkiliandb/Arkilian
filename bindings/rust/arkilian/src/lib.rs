use std::ffi::{CStr, CString};
use std::ptr;

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ROW: i32 = 100;
pub const SQLITE_DONE: i32 = 101;
pub const SQLITE_ERROR: i32 = 1;

mod bindings {
    use std::os::raw::c_char;
    
    #[repr(C)]
    pub struct Arkilian {
        _private: [u8; 0],
    }
    
    extern "C" {
        pub fn db_init(db: *mut *mut Arkilian, path: *const c_char) -> i32;
        pub fn db_close(db: *mut Arkilian);
        pub fn db_errmsg(db: *mut Arkilian) -> *const c_char;
        pub fn db_set_token(db: *mut Arkilian, token: *const c_char) -> i32;
        
        pub fn db_exec(db: *mut Arkilian, sql: *const c_char) -> i32;
        pub fn db_prepare(db: *mut Arkilian, sql: *const c_char) -> i32;
        pub fn db_use_stmt(db: *mut Arkilian, index: i32) -> i32;
        pub fn db_stmt_count(db: *mut Arkilian) -> i32;
        pub fn db_step(db: *mut Arkilian) -> i32;
        pub fn db_finalize(db: *mut Arkilian) -> i32;
        pub fn db_reset(db: *mut Arkilian) -> i32;
        pub fn db_column_count(db: *mut Arkilian) -> i32;
        pub fn db_column_name(db: *mut Arkilian, col: i32) -> *const c_char;
        pub fn db_column_text(db: *mut Arkilian, col: i32) -> *const c_char;
        pub fn db_column_int(db: *mut Arkilian, col: i32) -> i32;
        pub fn db_column_double(db: *mut Arkilian, col: i32) -> f64;
        
        pub fn db_bind_text(db: *mut Arkilian, idx: i32, val: *const c_char) -> i32;
        pub fn db_bind_int(db: *mut Arkilian, idx: i32, val: i32) -> i32;
        pub fn db_bind_double(db: *mut Arkilian, idx: i32, val: f64) -> i32;
    }
}

pub struct Database {
    ptr: *mut bindings::Arkilian,
}

impl Database {
    pub fn new(path: &str) -> Result<Self, String> {
        let c_path = CString::new(path).map_err(|_| "Invalid path")?;
        let mut db_ptr: *mut bindings::Arkilian = ptr::null_mut();
        
        let result = unsafe { bindings::db_init(&mut db_ptr, c_path.as_ptr()) };
        
        if result != SQLITE_OK {
            let err = unsafe {
                if !db_ptr.is_null() {
                    CStr::from_ptr(bindings::db_errmsg(db_ptr))
                        .to_string_lossy()
                        .into_owned()
                } else {
                    "Failed to initialize database".to_string()
                }
            };
            return Err(err);
        }
        
        Ok(Database { ptr: db_ptr })
    }
    
    pub fn close(&mut self) {
        if !self.ptr.is_null() {
            unsafe { bindings::db_close(self.ptr) };
            self.ptr = ptr::null_mut();
        }
    }
    
    pub fn set_token(&self, token: &str) -> Result<(), String> {
        let c_token = CString::new(token).map_err(|_| "Invalid token")?;
        let result = unsafe { bindings::db_set_token(self.ptr, c_token.as_ptr()) };
        if result != SQLITE_OK {
            return Err("Failed to set account token".to_string());
        }
        Ok(())
    }
    
    pub fn exec(&self, sql: &str) -> Result<i32, String> {
        let c_sql = CString::new(sql).map_err(|_| "Invalid SQL")?;
        let result = unsafe { bindings::db_exec(self.ptr, c_sql.as_ptr()) };
        
        if result != SQLITE_OK && result != SQLITE_DONE {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn prepare(&self, sql: &str) -> Result<i32, String> {
        let c_sql = CString::new(sql).map_err(|_| "Invalid SQL")?;
        let result = unsafe { bindings::db_prepare(self.ptr, c_sql.as_ptr()) };
        
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn use_stmt(&self, index: i32) -> Result<(), String> {
        let result = unsafe { bindings::db_use_stmt(self.ptr, index) };
        if result != SQLITE_OK {
            return Err("Invalid statement index or statement already finalized".to_string());
        }
        Ok(())
    }
    
    pub fn stmt_count(&self) -> i32 {
        unsafe { bindings::db_stmt_count(self.ptr) }
    }
    
    pub fn step(&self) -> i32 {
        unsafe { bindings::db_step(self.ptr) }
    }
    
    pub fn finalize(&self) -> Result<i32, String> {
        let result = unsafe { bindings::db_finalize(self.ptr) };
        
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn reset(&self) -> Result<i32, String> {
        let result = unsafe { bindings::db_reset(self.ptr) };
        
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn column_count(&self) -> i32 {
        unsafe { bindings::db_column_count(self.ptr) }
    }
    
    pub fn column_name(&self, col: i32) -> Option<String> {
        unsafe {
            let ptr = bindings::db_column_name(self.ptr, col);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
            }
        }
    }
    
    pub fn column_text(&self, col: i32) -> Option<String> {
        unsafe {
            let ptr = bindings::db_column_text(self.ptr, col);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
            }
        }
    }
    
    pub fn column_int(&self, col: i32) -> i32 {
        unsafe { bindings::db_column_int(self.ptr, col) }
    }
    
    pub fn column_double(&self, col: i32) -> f64 {
        unsafe { bindings::db_column_double(self.ptr, col) }
    }
    
    pub fn bind_text(&self, idx: i32, value: &str) -> Result<i32, String> {
        let c_val = CString::new(value).map_err(|_| "Invalid value")?;
        let result = unsafe { bindings::db_bind_text(self.ptr, idx, c_val.as_ptr()) };
        
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn bind_int(&self, idx: i32, value: i32) -> Result<i32, String> {
        let result = unsafe { bindings::db_bind_int(self.ptr, idx, value) };
        
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn bind_double(&self, idx: i32, value: f64) -> Result<i32, String> {
        let result = unsafe { bindings::db_bind_double(self.ptr, idx, value) };
        
        if result != SQLITE_OK {
            return Err(self.last_error());
        }
        Ok(result)
    }
    
    pub fn run(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<(), String> {
        self.prepare(sql)?;
        
        for (i, param) in params.iter().enumerate() {
            param.bind(self, (i + 1) as i32)?;
        }
        
        let _ = self.step(); // Step returns row code, ignore for non-SELECT
        self.finalize()?;
        
        Ok(())
    }
    
    pub fn all(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Vec<(String, String)>>, String> {
        self.prepare(sql)?;
        
        for (i, param) in params.iter().enumerate() {
            param.bind(self, (i + 1) as i32)?;
        }
        
        let col_count = self.column_count();
        let column_names: Vec<String> = (0..col_count)
            .filter_map(|i| self.column_name(i))
            .collect();
        
        let mut results = Vec::new();
        
        while self.step() == SQLITE_ROW {
            let mut row = Vec::new();
            for (i, name) in column_names.iter().enumerate() {
                if let Some(value) = self.column_text(i as i32) {
                    row.push((name.clone(), value));
                }
            }
            results.push(row);
        }
        
        self.finalize()?;
        
        Ok(results)
    }
    
    fn last_error(&self) -> String {
        unsafe {
            CStr::from_ptr(bindings::db_errmsg(self.ptr))
                .to_string_lossy()
                .into_owned()
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

pub trait ToSql {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String>;
}

impl ToSql for str {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_text(idx, self)
    }
}

impl ToSql for String {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_text(idx, self.as_str())
    }
}

impl ToSql for i32 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_int(idx, *self)
    }
}

impl ToSql for f64 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_double(idx, *self)
    }
}

impl ToSql for &str {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_text(idx, self)
    }
}

impl ToSql for &i32 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_int(idx, **self)
    }
}

impl ToSql for &f64 {
    fn bind(&self, db: &Database, idx: i32) -> Result<i32, String> {
        db.bind_double(idx, **self)
    }
}