IN ORDER TO GENERATE DATA:
  ```
    CD to the GeneratingCode folder:
    FOR CONCERT DATA:
          Run the generateConcert code by using
            py generateConcert.py
            with optional arguments
              --path "PUT YOUR PATH IN HERE" (default is ../input/)
              --covidchance inputChance (default is .01)
              --size inputSizeInMB (default is 10)
           
  
  
    FOR TRANSACTIONDATA:
      Run the generateTransactions code by using
        py generateTransactions.py
          with optional arguments
            --path "PUT YOUR PATH IN HERE" (default is ../input/)
            --customers int (default is 50000)
            --purchases int (default is 5000000)
            ```