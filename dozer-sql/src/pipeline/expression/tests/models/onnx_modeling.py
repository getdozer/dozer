import torch


class Sum(torch.nn.Module):
    def __init__(self):
        super().__init__()
        torch.nn.Sequential()

    def forward(self, x):
        return torch.sum(x)[None]


model = torch.nn.Sequential()

dummy_input = torch.randn(4)
model.add_module(name='sum_output', module=Sum())

print(model)
print(model(torch.ones(4)))

# Exporting to ONNX format
torch.onnx.export(model, torch.ones(4), "sum.onnx")
