# Cumulus RSync Agent (CuRSA)
 
A Flask REST API for Cumulus
This module of Cumulus will transfer all the data to the server, using RSync to reduce the amount of transfer when it's possible.
The reason for a separate module, rather than transferring the data from the client, is to avoid parallel transfers, as well as to make sure the files are transferred from the best place in your local network. For instance, if your data are stored on a shared folder in the network, it is best to run CuRSA directly on the server hosting that shared folder and transfer the data from there.
Another reason is that RSync requires a certificate to connect to the server, using a separate module avoids sharing that certificate with the users.


## Installation

To run the Cumulus RSync Agent work, you need to:

* Provide a working certificate to contact the controller where Cumulus_server is listening. At the moment, the expected certificate cannot require a password.
* Make sure to have RSync installed and added to the PATH. On Windows machines, use https://itefix.net/cwrsync
* Fill the cumulus_rsync.conf file
* It is advised to create a service to make sure the agent will be working at all times. On Windows, you can use NSSM: https://nssm.cc/


