from tables.account import Account
from tables.subdivision import Subdivision

subdiv_name_id_dict = Subdivision().run()
Account().run(subdiv_name_id_dict)


