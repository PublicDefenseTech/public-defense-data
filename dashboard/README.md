## Local Data Dashboard

This dashboard provides a simple, visual representation of local data tables within an HTML file.

**(Future Enhancement: Enhanced log file readability and management capabilities will be added.)**

---

### Running the Dashboard

Follow these steps to launch the dashboard:

1.  **Configure Database Credentials (.env file):**
    * Create a `.env` file within the `/dashboard` folder.
    * Add your database connection details in the following format:

        ```
        PGUSER=your user
        PGPASSWORD=your password
        PGDATABASE=your database
        PGHOST=localhost
        ```

2.  **Activate your Python Environment:**
    * Ensure your Python virtual environment (if used) is activated.

3.  **Navigate to the Dashboard Directory:**
    * Open your terminal or command prompt.
    * Use the `cd` command to navigate to the `/dashboard` folder.

4.  **Start the Server:**
    * Execute the command `python server.py`.
    * This will start the dashboard server, typically on port 5000.

5.  **Access the Dashboard:**
    * Open your web browser.
    * Enter the address `http://localhost:5000/` in the address bar and press Enter.

Log Data Page Example
![image](https://github.com/user-attachments/assets/cfa6dc07-b94f-49e6-a1e2-b151727d8aaf)

Database Table Viewer Example
![image](https://github.com/user-attachments/assets/0ca1c8df-39c7-454c-a28d-7289cc8e50ea)
