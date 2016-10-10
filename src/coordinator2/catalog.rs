use std::cell::RefCell;
use std::rc::Rc;

use model::*;

pub struct Catalog {

}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            
        }
    }
}

pub struct CatalogMut {
    catalog: Rc<RefCell<Catalog>>,
}

impl From<Catalog> for CatalogMut {
    fn from(catalog: Catalog) -> Self {
        CatalogMut {
            catalog: Rc::new(RefCell::new(catalog))
        }
    }
}
