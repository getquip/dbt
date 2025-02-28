# About
This repo is for all things related to dbt.

## Getting Started  

1. **Create a Virtual Environment**  
    A virtual environment isolates your project dependencies from the system Python installation, preventing version conflicts and ensuring a consistent development environment.
   
   Run the following command in your terminal to set up a virtual environment:  
   ```bash
   python3 -m venv venv
   ```

2. **Activate the Virtual Environment**  
   Use the appropriate command for your operating system:  
   - On macOS/Linux:  
     ```bash
     source venv/bin/activate
     ```  
   - On Windows:  
     ```bash
     venv\Scripts\activate
     ```

3. **Install Dependencies**  
   Install the required packages by running:  
   ```bash
   pip3 install -r requirements.txt
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
