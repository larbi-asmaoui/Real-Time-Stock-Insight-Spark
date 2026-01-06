import torch
import pyarrow.parquet as pq
import os
import glob
import numpy as np
from torch.utils.data import IterableDataset

class StockDataset(IterableDataset):
    """
    Memory-efficient PyTorch Dataset for streaming Stock data from Parquet files.
    Designed to prevent Driver OOM by reading files sequentially/lazily.
    """
    def __init__(self, data_dir):
        """
        Args:
            data_dir: Directory containing parquet files.
        """
        self.file_paths = glob.glob(os.path.join(data_dir, "*.parquet"))
        if not self.file_paths:
            # Try recursive or assume dir itself if it's a spark output dir
            self.file_paths = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
            # Filter out _SUCCESS etc
            self.file_paths = [p for p in self.file_paths if not p.endswith("_SUCCESS")]

    def __iter__(self):
        """
        Yields:
            (features, target)
        """
        for file_path in self.file_paths:
            try:
                # Read one file at a time using PyArrow
                # We interpret 'features' column as the input sequence X, and 'target' as y
                parquet_file = pq.ParquetFile(file_path)
                
                for batch in parquet_file.iter_batches():
                    df_batch = batch.to_pandas()
                    
                    if 'features' in df_batch.columns and 'target' in df_batch.columns:
                        features = df_batch['features'].values
                        targets = df_batch['target'].values
                        
                        for f, t in zip(features, targets):
                            # f is expected to be a list of lists (seq_len, num_features) or flattened
                            # We assume it matches the shape required for the model
                            # Convert to numpy then tensor
                            f_np = np.array(f) 
                            t_np = np.array(t)
                            
                            yield torch.tensor(f_np, dtype=torch.float32), torch.tensor(t_np, dtype=torch.float32).unsqueeze(0)
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                continue
