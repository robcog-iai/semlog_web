from image_path.utils import absoluteFilePaths
from torchvision import transforms
from torch.utils.data import Dataset
from PIL import Image


class ClassifierDataset(Dataset):
    """Pytorch dataset class for multi-class classifier."""
    def __init__(self, image_folder):

        self.image_path_list = absoluteFilePaths(image_folder)
        self.class_list=list(set([i.split("_")[2].split(".")[0] for i in self.image_path_list]))
        self.label_list = []
        self.transform = transforms.Compose([
            # Enforce resizing for evaluation. Remove to train for different resolution.
            transforms.Resize([80,80]),
            transforms.ToTensor(),
        ])
        for p in self.image_path_list:
            for class_name in self.class_list:
                if class_name in p:
                    ind = self.class_list.index(class_name)
                    self.label_list.append(ind)

    def __len__(self):
        return len(self.image_path_list)

    def __getitem__(self, idx):
        image = Image.open(self.image_path_list[idx])
        image = self.transform(image)
        label = self.label_list[idx]
        return image,label

    def get_class_list(self):
        return self.class_list
