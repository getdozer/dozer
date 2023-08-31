import torch


# network class 2-hidden layer model
class NeuralNetwork(torch.nn.Module):
    def __init__(self):
        super().__init__()

    def forward(self, x):
        return x.sum()


model = NeuralNetwork()

dummy_input = torch.randn(4)

# Exporting to ONNX format
torch.onnx.export(model, dummy_input, "sum.onnx")
