// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Owned graph composition without projecting through JSON. References are
//! remapped as one table, preserving cycles, views, errors and shared identity.

use super::*;

impl StructuredValue {
    pub fn undefined() -> Self {
        Self {
            root: Value::Undefined,
            objects: Vec::new(),
        }
    }

    /// Read a plain object's own UTF-16 property. The returned graph owns its
    /// table and retains every reference reachable from the property.
    pub fn property(&self, name: &[u16]) -> Result<Option<Self>> {
        let properties = self.properties()?;
        Ok(properties.iter().rev().find_map(|(key, value)| {
            matches!(key, Value::String(key) if key == name).then(|| Self {
                root: value.clone(),
                objects: self.objects.clone(),
            })
        }))
    }

    pub fn set_property(&mut self, name: Vec<u16>, value: &Self) -> Result<()> {
        let properties = self.properties()?;
        ensure!(
            properties.len() < Limits::default().values
                || properties
                    .iter()
                    .any(|(key, _)| matches!(key,Value::String(key) if key==&name)),
            "structured object exceeds property limit"
        );
        let imported = self.import(value)?;
        let properties = self.properties_mut()?;
        if let Some((_, value)) = properties
            .iter_mut()
            .rev()
            .find(|(key, _)| matches!(key, Value::String(key) if key == &name))
        {
            *value = imported;
        } else {
            properties.push((Value::String(name), imported));
        }
        Ok(())
    }

    pub fn remove_property(&mut self, name: &[u16]) -> Result<bool> {
        let properties = self.properties_mut()?;
        let before = properties.len();
        properties.retain(|(key, _)| !matches!(key, Value::String(key) if key == name));
        Ok(before != properties.len())
    }

    pub fn own_properties(&self) -> Result<&Properties> {
        self.properties()
    }

    pub fn array(values: impl IntoIterator<Item = Self>) -> Result<Self> {
        let mut output = Self {
            root: Value::Reference(0),
            objects: vec![Object::DenseArray {
                elements: Vec::new(),
                properties: Vec::new(),
            }],
        };
        let mut elements = Vec::new();
        for value in values {
            ensure!(
                elements.len() < Limits::default().values,
                "structured array exceeds value limit"
            );
            elements.push(output.import(&value)?);
        }
        let Object::DenseArray {
            elements: destination,
            ..
        } = &mut output.objects[0]
        else {
            unreachable!()
        };
        *destination = elements;
        Ok(output)
    }

    /// Append an independent table and return its relocated root. Failure
    /// leaves this graph unchanged. Importing the same graph twice makes two
    /// independent graphs, as two structured-clone operations do.
    pub fn import(&mut self, value: &Self) -> Result<Value> {
        let count = self
            .objects
            .len()
            .checked_add(value.objects.len())
            .ok_or_else(|| anyhow!("structured graph object count overflow"))?;
        ensure!(
            count <= Limits::default().objects,
            "structured graph exceeds object limit"
        );
        let offset = u32::try_from(self.objects.len())?;
        let size = value.objects.len();
        let reference = |id: &mut u32| -> Result<()> {
            ensure!((*id as usize) < size, "invalid structured graph reference");
            *id = id
                .checked_add(offset)
                .ok_or_else(|| anyhow!("structured graph reference overflow"))?;
            Ok(())
        };
        let relocate = |value: &mut Value| -> Result<()> {
            if let Value::Reference(id) = value {
                reference(id)?;
            }
            Ok(())
        };
        let properties = |properties: &mut Properties| -> Result<()> {
            for (key, value) in properties {
                relocate(key)?;
                relocate(value)?;
            }
            Ok(())
        };
        let mut root = value.root.clone();
        relocate(&mut root)?;
        let mut objects = value.objects.clone();
        for object in &mut objects {
            match object {
                Object::Object(p) | Object::Map(p) | Object::SparseArray { properties: p, .. } => {
                    properties(p)?
                }
                Object::DenseArray {
                    elements,
                    properties: p,
                } => {
                    for element in elements {
                        relocate(element)?;
                    }
                    properties(p)?;
                }
                Object::Set(values) => {
                    for value in values {
                        relocate(value)?;
                    }
                }
                Object::Boxed(value) => relocate(value)?,
                Object::View { buffer, .. } => reference(buffer)?,
                Object::Error {
                    cause: Some(value), ..
                } => relocate(value)?,
                _ => {}
            }
        }
        self.objects.extend(objects);
        Ok(root)
    }

    fn properties(&self) -> Result<&Properties> {
        let Value::Reference(id) = self.root else {
            bail!("structured value is not a plain object")
        };
        match self.objects.get(id as usize) {
            Some(Object::Object(properties)) => Ok(properties),
            _ => bail!("structured value is not a plain object"),
        }
    }
    fn properties_mut(&mut self) -> Result<&mut Properties> {
        let Value::Reference(id) = self.root else {
            bail!("structured value is not a plain object")
        };
        match self.objects.get_mut(id as usize) {
            Some(Object::Object(properties)) => Ok(properties),
            _ => bail!("structured value is not a plain object"),
        }
    }
}
