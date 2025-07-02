# GDHI Adjustment Pipeline

This project runs controlled adjustments of GDHI figures at LSOA levels and renormalises the LSOAs within LAD groups to keep the correct LAD total.

## Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/ONSdigital/gdhi_adj.git
   ```
2. **Install Python v. 3.12:**
    - Either use the script "Python Current Test" from Windows Software Center
    - or Install Miniconda via the Software centre and create a new Conda environment:
      ```sh
      conda create --name gdhi_adj_312 python=3.12
      ```

3. **For users: Install Spyder 6**
4. **For developers: install VS Code**
5. **Activate the virtual environment:**
   ```sh
   conda activate gdhi_adj_312
   ```
6. **Install the required packages:**
   ```sh
   pip install -r requirements.txt
   ```
7. **Install and run all pre-commits:**
   ```sh
   pre-commit install
   pre-commit run -a
   ```
8. **Review and edit `config/config.toml`**
9. **Run the file `main.py`**
