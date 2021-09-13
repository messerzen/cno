
import configparser

def credentials():
    """Returns the database credentials from cfg file.
    """
    config = configparser.ConfigParser()
    config.read('/home/zen/Desenvolvimento/Environments/cno/etl/credentials.cfg')

    admin_username = config['DATABASE_CREDENTIALS']['admin_username']
    admin_password = config['DATABASE_CREDENTIALS']['admin_password']         

    return admin_username, admin_password


print(credentials()[0], credentials()[1])