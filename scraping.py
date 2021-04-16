from twint import twint


username = "Aureoles"
c = twint.Config()
c.Search = username
c.Store_csv = True
c.Limit = 2000
c.Count = True
#c.Hide_output = True
#c.Profile_full = True
#c.Resume = True
c.Output = username+'_'+'.csv'

twint.run.Search(c)