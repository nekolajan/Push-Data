#@title: Push Data
#@description: Tool which stores data in Redshift
#@author: Jan Nekola

#Libraries
import pandas as pd
from tkinter import ttk, filedialog, messagebox
from tkinter import *
from datetime import datetime
from sqlalchemy import create_engine, exc


class App_Control:
     
    def __init__(self, master = None):

        # GUI
        self.master = master
        self.master.title('Push Data')
        self.master.configure(background='azure')
        self.master.minsize(550, 450)
        self.master.resizable(True, True)
        
        self.style = ttk.Style()
 
        self.style.configure('TFrame', background='azure')
        self.style.configure('TButton', background='LightSkyBlue1')
        self.style.configure('TLabel', background = 'azure', font = ('Arial', 11))
        self.style.configure('Header.TLabel', font = ('Arial', 20, 'bold'))

        self.frame_header = ttk.Frame(master)
        self.frame_header.pack()

        ttk.Label(self.frame_header, text = 'App which helps you to push data', style = 'Header.TLabel', foreground='#00004d').grid(row=0, column = 0)
        
        self.frame_rd_credentials = ttk.Frame(master)
        self.frame_rd_credentials.pack()
        ttk.Label(self.frame_rd_credentials, text = 'Redshift Credentials', font = ('Arial', 18)).grid(row=1, column = 0)

        self.frame_content = ttk.Frame(master)
        self.frame_content.pack()

        ttk.Label(self.frame_content, text = 'Username:').grid(row=0, column = 0, padx = 5, sticky = 'sw')
        ttk.Label(self.frame_content, text = 'Password:').grid(row=2, column = 0, padx = 5, sticky = 'sw')

        self.rs_username = ttk.Entry(self.frame_content, show = '*', width = 24, font = ('Arial', 10))
        self.rs_password = ttk.Entry(self.frame_content, show = '*', width = 24, font = ('Arial', 10))

        self.rs_username.grid(row=1, column = 0, padx = 5)
        self.rs_password.grid(row=3, column = 0, padx = 5)

        self.input_file = ttk.Frame(master)
        self.input_file.pack()
        
        #Varible to store selected file
        self.file = StringVar()
        self.file.set('--------------')

        ttk.Label(self.input_file, wraplength = 300,
                  text = ("Select File:")).grid(row = 0, column = 0)
        ttk.Button(self.input_file, text = 'Browse', command = self.openfile ).grid(row = 1, column = 0)
        ttk.Label(self.input_file, textvariable = self.file).grid(row=2, column = 0, padx = 5, sticky = 'sw')
        
        self.destination = ttk.Frame(master)
        self.destination.pack()
        
        #variable to choose replace or append data.
        self.var = StringVar()
        self.var.set('replace') # default value
        
        ttk.Label(self.destination, text = 'Schema Name:' ).grid(row=0, column = 0, padx = 5, sticky = 'sw')
        ttk.Label(self.destination, text = 'Table Name:' ).grid(row=2, column = 0, padx = 5, sticky = 'sw')
        ttk.OptionMenu(self.destination,self.var,'replace','replace', 'append').grid(row=4, column = 0, padx = 5, sticky = 'n')

        self.destination_schema = ttk.Entry(self.destination, width = 24, font = ('Arial', 10))
        self.destination_table = ttk.Entry(self.destination, width = 24, font = ('Arial', 10))
        
        self.destination_schema.grid(row=1, column = 0, padx = 5)
        self.destination_table.grid(row=3, column = 0, padx = 5)
        
        self.buttons = ttk.Frame(master)
        self.buttons.pack()
        
        ttk.Button(self.buttons, text = 'Submit', 
                   command = self.submit).grid(row = 0, column = 0, padx = 5, pady = 5, sticky = 'e')
        ttk.Button(self.buttons, text = 'Clear',
                   command = self.clear).grid(row = 1, column = 0, padx = 5, pady = 5, sticky = 'w')    
        
        
        self.bottom = ttk.Frame(master)
        self.bottom.pack()
        
        self.comments = StringVar()
        self.comments.set('--------------')
        
        ttk.Label(self.bottom, text = 'Status:' ).grid(row=0, column = 0, padx = 5)
        ttk.Label(self.bottom, textvariable = self.comments, background= 'mint cream').grid(row=1, column = 0, padx = 5)



    def openfile(self):       
        self.filename = filedialog.askopenfilename(initialdir = "/",title = 'Select file', filetypes = (('CSV files', '*.csv'),('Text files', '*.txt')))
        if self.filename.endswith('.csv'):
            self.file.set(self.filename)
        elif self.filename.endswith('.txt'):
            self.file.set(self.filename)
        elif (len(self.filename)==0 or self.filename == '--------------'):
            #file not selected
            self.file.set('--------------')
        else:
            messagebox.showwarning("Warning","Only TXT and CSV files are supported.")
            self.file.set('--------------')

    def ifexists(self):
        return self.var.get()
    
    def updatecomment(self):
        self.comments.set('In Progress')


    def submit(self):   
        #Update status
        self.updatecomment() 

        # check if the credentials were added
        if len(self.rs_username.get())<1:
            messagebox.showwarning("Warning","Insert Your Username.")
            self.comments.set('Insert Your Username.')
        if len(self.rs_password.get())<1:
            messagebox.showwarning("Warning","Insert Your Password.")
            self.comments.set('Insert Your Password.')
        # test if file was selected
        if self.file.get() == '--------------':
            messagebox.showwarning("Warning","File has been selected.")
            self.comments.set('File has been selected.')
        # check if schema and table have been inserted.
        if len(self.destination_schema.get()) <1:
            messagebox.showwarning("Warning","Insert Destination Schema.")  
            self.comments.set('Insert Destination Schema.')
        if len(self.destination_table.get()) <1:
            messagebox.showwarning("Warning","Insert Destination Table.")
            self.comments.set('Insert Destination Table.')
        if (len(self.rs_username.get())>0 and len(self.rs_password.get())>0 and len(self.file.get())>0 and len(self.destination_schema.get())>0 and len(self.destination_table.get())>0):
            self.comments.set('Connecting to Redshift')
            #self.redshift_conn()
            self.master.after(10, self.redshift_conn())


    def clear(self):         
        self.rs_username.delete(0, 'end')
        self.rs_password.delete(0, 'end')
        self.file.set('--------------')
        self.destination_table.delete(0, 'end')
        self.destination_schema.delete(0, 'end')
        self.comments.set('--------------')

        
    def redshift_conn(self):       
        self.start = datetime.now()
        try:  
            #change _server_ with your server  
            self.engine = create_engine('postgres+psycopg2://'+self.rs_username.get()+':'+self.rs_password.get()+'_server_', connect_args={'sslmode':'require'}, echo=False)
            self.conn = self.engine.connect()
            
            if self.filename.endswith('.csv'):
                for chunk in pd.read_csv(self.filename.replace("\\", "/"), chunksize = 10000, encoding='utf-8'):
                    chunk.to_sql(self.destination_table.get(), self.conn, if_exists=self.ifexists(), schema = self.destination_schema.get(), index = False, chunksize = 1000)
                    del chunk
            if self.filename.endswith('.txt'):
                for chunk in pd.read_csv(self.filename.replace("\\", "/"), chunksize = 10000, delimiter='\t'):
                    chunk.to_sql(self.destination_table.get(), self.conn, if_exists=self.ifexists(), schema = self.destination_schema.get(), index = False, chunksize = 1000)
                    del chunk                
        except exc.OperationalError as e:
            messagebox.showwarning("Error","Login failed wrong user credentials")
            self.comments.set('--------------')
        finally:
            #Grant permission - replace self.rs_username.get() to different user who should have access to newly created table
            self.conn.execute('GRANT ALL ON hr_emea.' + self.destination_table.get() +' to' + self.rs_username.get() + ';')
            self.conn.close()
        
        self.comments.set('Total {} minutes'.format((datetime.now() - self.start).seconds/60))


def main():
    
    root = Tk()
    root.iconbitmap('logo.ico')
    App_Control(root)   
    root.mainloop()
    

if __name__== "__main__":
    main()
