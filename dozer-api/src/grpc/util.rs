use prost_reflect::{DescriptorPool, MethodDescriptor};

pub fn get_service_name(descriptor: DescriptorPool) -> String {
    let service_lst = descriptor.services().next().unwrap();
    let service_name = service_lst.full_name();
    return service_name.to_string();
}

pub fn get_method_by_name(
    descriptor: DescriptorPool,
    method_name: String,
) -> Option<MethodDescriptor> {
    let service_lst = descriptor.services().next().unwrap();
    let mut methods = service_lst.methods();
    let method_descriptor = methods.find(|m| m.name().to_string() == method_name);
    return method_descriptor;
}

