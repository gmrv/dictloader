from tables.access import Access
from tables.account import Account
from tables.subdivision import Subdivision

subdiv_name_id_dict = Subdivision().run()
fio_id_dict = Account().run(subdiv_name_id_dict)
Access().run(subdiv_name_id_dict, fio_id_dict)


