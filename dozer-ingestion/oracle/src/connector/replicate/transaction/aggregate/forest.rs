use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};

#[derive(Debug, Clone)]
struct Node<Id, T> {
    data: T,
    parent: Option<Id>,
    children: Vec<Id>,
}

impl<Id, T: Default> Default for Node<Id, T> {
    fn default() -> Self {
        Self {
            data: T::default(),
            parent: None,
            children: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Forest<Id, T> {
    nodes: HashMap<Id, Node<Id, T>>,
}

impl<Id, T> Default for Forest<Id, T> {
    fn default() -> Self {
        Self {
            nodes: HashMap::default(),
        }
    }
}

impl<Id: Eq + Hash, T> Forest<Id, T> {
    pub fn remove_subtree(&mut self, id: Id, mut f: impl FnMut(Id, T)) -> bool {
        let Some(node) = self.nodes.remove(&id) else {
            return false;
        };
        if let Some(parent) = node.parent.as_ref() {
            self.nodes
                .get_mut(parent)
                .unwrap()
                .children
                .retain(|child| child != &id);
        }
        let mut stack = vec![(id, node)];
        while let Some((id, node)) = stack.pop() {
            f(id, node.data);
            for child in node.children {
                let node = self.nodes.remove(&child).unwrap();
                stack.push((child, node));
            }
        }
        true
    }

    pub fn get_mut(&mut self, id: &Id) -> Option<&mut T> {
        self.nodes.get_mut(id).map(|node| &mut node.data)
    }
}

impl<Id: Eq + Hash, T: Default> Forest<Id, T> {
    pub fn insert_or_get_root(&mut self, id: Id) -> &mut T {
        &mut self.nodes.entry(id).or_default().data
    }
}

impl<Id: Eq + Hash + Clone, T: Default> Forest<Id, T> {
    pub fn insert_or_get_child(&mut self, parent: Id, child: Id) -> Option<&mut T> {
        if !self.nodes.contains_key(&parent) {
            return None;
        }

        let is_new_child = if let Entry::Vacant(entry) = self.nodes.entry(child.clone()) {
            entry.insert(Node {
                data: T::default(),
                parent: Some(parent.clone()),
                children: vec![],
            });
            true
        } else {
            false
        };

        if is_new_child {
            self.nodes
                .get_mut(&parent)
                .unwrap()
                .children
                .push(child.clone());
        }

        Some(&mut self.nodes.get_mut(&child).unwrap().data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_forest() {
        let mut forest = Forest::<u32, Vec<()>>::default();
        let node1 = forest.insert_or_get_root(1);
        assert_eq!(node1, &vec![]);
        node1.push(());
        assert_eq!(forest.insert_or_get_root(2), &vec![]);
        assert_eq!(forest.insert_or_get_child(0, 3), None);
        let node3 = forest.insert_or_get_child(1, 3).unwrap();
        assert_eq!(node3, &vec![]);
        node3.extend([(), ()]);
        let mut collected = vec![];
        forest.remove_subtree(1, |_, data| collected.extend(data));
        assert_eq!(collected.len(), 3);
        assert_eq!(forest.insert_or_get_root(1), &vec![]);
    }
}
