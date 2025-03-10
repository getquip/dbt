# About
This repo is for all things related to dbt.

## Getting Started  

#### Poetry for Python
Poetry is a Python dependency management tool that allows us to manage your project’s dependencies, packaging, and publishing with ease. Follow the steps below to install and set up Poetry on your local machine.

**Prerequisites**
Python 3.10+ should be installed on your system. You can check your Python version with:
```bash
python --version
```

If you need to install Python, follow the official [Python installation](https://www.python.org/downloads/) guide.
Next, follow the official guide for [Installing Poetry](https://python-poetry.org/docs/).

1. **Create a Virtual Environment**  
    A virtual environment isolates your project dependencies from the system Python installation, preventing version conflicts and ensuring a consistent development environment.
   
   Run the following command in your terminal to set up a virtual environment:  
   ```bash
   poetry env use python3.10
   ```

2. **Activate the Virtual Environment**  
     ```bash
     eval $(poetry env activate)
     ```  =

3. **Start your Virtual Environment**  =
   ```bash
   eval $(poetry env activate)
   ```  

---  

### Instructions on how to set DBT_GCP_BQ_DATASET:


   To make this change persistent across terminal sessions, you need to update your `~/.zsh_profile` or `~/.zshrc` file (whichever file is sourced by your shell).

   1. Open the appropriate file in a text editor:

   ```zsh
   nano ~/.zsh_profile
   ```
   2. Update `DBT_GCP_BQ_DATASET` to `your_name`. Then save the file and confirm changes.

   3. After updating the file, run the following command to apply the changes.

   ```zsh
   source ~/.zsh_profile
   ```

   Or

   ```zsh
   source ~/.zshrc
   ```

4. To confirm that the variable has been updated, run:

   ```zsh
   echo $DBT_GCP_BQ_DATASET
   ```