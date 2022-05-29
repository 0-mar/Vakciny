# Imports
import datetime
from operator import itemgetter

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, MetaData, Table, distinct, desc
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define database tables here
meta = MetaData()

penguins = Table(
    "Penguin", meta,
    Column("PenguinID", Integer, primary_key=True),
    Column("FirstName", String(255), nullable=False),
    Column("LastName", String(255), nullable=False),
    Column("Birthday", DateTime, nullable=False),
    Column("District", Integer, nullable=False),
    Column("VaccineNumber", Integer, nullable=False),
    Column("PenguinPriority", Integer, nullable=False)
)

vaccination_centers = Table(
    "VaccinationCenter", meta,
    Column("CenterID", Integer, primary_key=True),
    Column("District", Integer, nullable=False),
    Column("WorkFrom", DateTime, nullable=False),
    Column("WorkTill", DateTime, nullable=False),
    Column("FreeVaccines", Integer, nullable=False)
)

valid_centers = Table(
    "ValidCenters", meta,
    Column("PenguinID", ForeignKey("Penguin.PenguinID"), primary_key=True),
    Column("CenterID", Integer, primary_key=True),
)

valid_times = Table(
    "ValidTimes", meta,
    Column("PenguinID", ForeignKey("Penguin.PenguinID"), primary_key=True),
    Column("Day", Integer, primary_key=True),
    Column("From", DateTime, nullable=False),
    Column("To", DateTime, nullable=False)
)

waiting_list = Table(
    "WaitingList", meta,
    Column("RegistrationID", Integer, primary_key=True),
    Column("PenguinID", ForeignKey("Penguin.PenguinID"))
)

timetable = Table(
    "TimeTable", meta,
    Column("RegistrationID", Integer, primary_key=True),
    Column("VaccinationCenterID", ForeignKey("VaccinationCenter.CenterID")),
    Column("Time", DateTime, nullable=False),
    Column("PenguinID", ForeignKey("Penguin.PenguinID")),
)

vaccination_log = Table(
    "VaccinationLog", meta,
    Column("RegistrationID", Integer, primary_key=True),
    Column("PenguinID", Integer, nullable=False),
    Column("VaccinationNumber", Integer, nullable=False),
    Column("VaccinationCenter", Integer, nullable=False),
    Column("VaccinationTime", DateTime, nullable=False)
)

Base = declarative_base()


class Penguin(Base):
    __tablename__ = "Penguin"

    penguin_id = Column("PenguinID", Integer, primary_key=True)
    first_name = Column("FirstName", String(255), nullable=False)
    last_name = Column("LastName", String(255), nullable=False)
    birthday = Column("Birthday", DateTime, nullable=False)
    district = Column("District", Integer, nullable=False)
    vaccine_number = Column("VaccineNumber", Integer, nullable=False)
    penguin_priority = Column("PenguinPriority", Integer, nullable=False)

    def __repr__(self):
        return "<" + str(self.penguin_id) + " " + str(self.first_name) + " " + str(
            self.last_name) + " with prio: " + str(self.penguin_priority) + ">"


class VaccinationCenter(Base):
    __tablename__ = "VaccinationCenter"

    center_id = Column("CenterID", Integer, primary_key=True)
    district = Column("District", Integer, nullable=False)
    work_from = Column("WorkFrom", DateTime, nullable=False)
    work_till = Column("WorkTill", DateTime, nullable=False)
    free_vaccines = Column("FreeVaccines", Integer, nullable=False)


class ValidCenters(Base):
    __tablename__ = "ValidCenters"

    penguin_id = Column("PenguinID", ForeignKey(Penguin.penguin_id), primary_key=True)
    center_id = Column("CenterID", Integer, primary_key=True)

    def __repr__(self):
        return "<" + str(self.penguin_id) + " " + str(self.center_id) + ">"


class ValidTimes(Base):
    __tablename__ = "ValidTimes"

    penguin_id = Column("PenguinID", ForeignKey(Penguin.penguin_id), primary_key=True)
    day = Column("Day", Integer, primary_key=True)
    from_time = Column("From", DateTime, nullable=False)
    to_time = Column("To", DateTime, nullable=False)


class WaitingList(Base):
    __tablename__ = "WaitingList"

    registration_id = Column("RegistrationID", Integer, primary_key=True)
    penguin_id = Column("PenguinID", ForeignKey(Penguin.penguin_id))


class TimeTable(Base):
    __tablename__ = "TimeTable"

    registration_id = Column("RegistrationID", Integer, primary_key=True)
    vaccination_center_id = Column("VaccinationCenterID", ForeignKey(VaccinationCenter.center_id))
    time = Column("Time", DateTime, nullable=False)
    penguin_id = Column("PenguinID", ForeignKey(Penguin.penguin_id))


class VaccinationLog(Base):
    __tablename__ = "VaccinationLog"

    registration_id = Column("RegistrationID", Integer, primary_key=True)
    penguin_id = Column("PenguinID", Integer, nullable=False)
    vaccination_number = Column("VaccinationNumber", Integer, nullable=False)
    vaccination_center = Column("VaccinationCenter", Integer, nullable=False)
    vaccination_time = Column("VaccinationTime", DateTime, nullable=False)


# Poznámka k řešení: Vím, že v zadání bylo řečeno, že se má použít WRITE mód k zápisu. Já však použil append spolu s
# with klauzulí proto, že si myslím, že je to lepší. Pokud se totiž něco stane (např. výpadek proudu), toto řešení je
# lepší než to, které používá WRITE, protože se soubor sám zavře. Avšak i tak zde WRITE používám - na začátku funkce
# databaseOperator, abych vymazal případný obsah souboru a také po zavolání changefile ze stejného důvodu. To znamená,
# že ve výsledku zapisování funguje úplně stejně.

def databaseOperator(vaccinationsLimit, currentDate, oldAge, printFile):
    engine = create_engine('sqlite:///:memory:')

    meta.create_all(engine)
    session = sessionmaker(bind=engine)()

    # Každému databázovému příkazu odpovídá jedna stejnojmenná funkce.
    # Pomocné funkce, které s hlavními nějak souvisejí, mají většinou jako prefix jméno hlavní,
    # např. registerpenguin -> pomocná funkce registerpenguin_all_centres

    with open(printFile, 'w') as writer:
        writer.write("")

    while True:
        newCommand = yield
        split_command = newCommand.split(" ")  # Rozkouskuje příkaz na jednotlivé části. Předává se jako argument
        operator = split_command[0]  # určuje příkaz

        if operator == "CREATECENTER":
            createcenter(session, split_command)

        elif operator == "CREATEPENGUIN":
            today = datetime.datetime.now().date()
            createpenguin(session, split_command, today, oldAge, vaccinationsLimit)

        elif operator == "REGISTERPENGUIN":
            registerpenguin(session, split_command, vaccinationsLimit)

        elif operator == "CHANGEREGISTRATIONCENTERS":
            changeregistrationcentres(session, split_command)

        elif operator == "CHANGEREGISTRATIONTIMES":
            changeregistrationtimes(session, split_command)

        elif operator == "CHANGEFILE":
            printFile = changefile(split_command)
            with open(printFile, 'w') as writer:
                writer.write("")

        elif operator == "PRINTREGISTERED":
            printregistered(session, split_command, printFile)

        elif operator == "PRINTFREECENTERS":
            printfreecenters(session, split_command, printFile)

        elif operator == "PRINTVALIDTIMES":
            printvalidtimes(session, split_command, printFile)

        elif operator == "ENDDAY":
            endday(session, split_command, vaccinationsLimit, printFile)

        elif operator == "FINDAPPOINTMENTS":
            findappointments(session, split_command, printFile)

        elif operator == "FINDLOGGEDVACCINATIONS":
            findloggedvaccinations(session, split_command, printFile)

        elif operator == "GIVESTATISTICS":
            givestatistics(session, printFile, vaccinationsLimit)


# --------------------------------------------------------------------------------------------------------
# FUNKCE JEDNODUCHYCH DOTAZU
# --------------------------------------------------------------------------------------------------------


def createcenter(session, split_command):
    """Funkce vytvoří centrum a přidá ho do databáze"""
    from_time = split_command[3] + " " + split_command[4]
    to_time = split_command[5] + " " + split_command[6]

    from_time = datetime.datetime.strptime(from_time, "%H %M")
    to_time = datetime.datetime.strptime(to_time, "%H %M")

    vac_center = VaccinationCenter(center_id=int(split_command[1]), district=int(split_command[2]),
                                   work_from=from_time,
                                   work_till=to_time,
                                   free_vaccines=int(split_command[7]))
    session.add(vac_center)
    session.commit()
    # print(session.query(VaccinationCenter).all())


def createpenguin(session, split_command, currentDate, oldAge, vaccinationsLimit):
    """Funkce vytvoří tučňáka a přidá ho do databáze"""
    bday = datetime.datetime.strptime(split_command[4], "%Y-%m-%d").date()
    # print(bday)
    if (currentDate - bday).days < 0 and (currentDate - bday).days > (30 * 365):
        return

    prio = calc_penguin_prio((currentDate - bday).days, oldAge, vaccinationsLimit,
                             int(split_command[6]), int(split_command[7]))

    penguin = Penguin(penguin_id=int(split_command[1]), first_name=split_command[2], last_name=split_command[3],
                      birthday=bday, district=int(split_command[5]), vaccine_number=int(split_command[6]),
                      penguin_priority=prio)
    session.add(penguin)
    session.commit()


def registerpenguin(session, split_command, vaccinationsLimit):
    """Funkce zaregistruje daného tučňáka do databáze, tzn. přidá ho na WaitingList a nastaví mu ValidCenters a
    ValidTimes """
    penguin = session.query(Penguin).get(split_command[1])

    if penguin.vaccine_number >= vaccinationsLimit:
        return

    reg_id = 0
    if session.query(WaitingList).all() != []:
        reg_id = session.query(func.max(WaitingList.registration_id)).all()[0][0] + 1

    wl = WaitingList(registration_id=reg_id, penguin_id=penguin.penguin_id)

    session.add(wl)
    session.commit()

    continue_index = 3
    arg1 = split_command[2]
    if arg1 == "ALL":
        registerpenguin_all_centres(session, penguin)
        arg = split_command[3]
        if arg == "CENTERS":
            continue_index = registerpenguin_selected_centres(session, penguin, split_command, 3)

    elif arg1 == "CENTERS":
        continue_index = registerpenguin_selected_centres(session, penguin, split_command, 2)

    arg2 = split_command[continue_index]

    while True:
        if arg2 == "ALWAYS":
            registerpenguin_whole_selected_days(session, penguin, [day for day in range(0, 7)])
            continue_index = continue_index + 1

        else:
            determiner = None
            try:
                determiner = split_command[continue_index + 2]
            except IndexError:
                registerpenguin_whole_selected_days(session, penguin, [int(split_command[continue_index + 1])])
                return

            if determiner == "DAY":
                registerpenguin_whole_selected_days(session, penguin, [int(split_command[continue_index + 1])])
                continue_index = continue_index + 2

            else:
                from_time = split_command[continue_index + 2] + " " + split_command[continue_index + 3]
                to_time = split_command[continue_index + 4] + " " + split_command[continue_index + 5]

                from_time = datetime.datetime.strptime(from_time, "%H %M")
                to_time = datetime.datetime.strptime(to_time, "%H %M")

                registerpenguin_selected_days_and_times(session, penguin, [[int(split_command[continue_index + 1]),
                                                                            from_time, to_time]])
                continue_index = continue_index + 6

        try:
            arg2 = split_command[continue_index]
        except IndexError:
            break


def changeregistrationcentres(session, split_command):

    for i in range(2, len(split_command)):
        if split_command[i].startswith("-"):
            trimmed_id = split_command[i].strip("-")

            if len(session.query(ValidCenters).filter(ValidCenters.penguin_id == int(split_command[1])).all()) > 1:
                vc = session.query(ValidCenters).get((int(split_command[1]), int(trimmed_id)))
                session.delete(vc)
                session.commit()

        else:
            vc = ValidCenters(penguin_id=int(split_command[1]), center_id=int(split_command[i]))
            session.add(vc)
            session.commit()


def changeregistrationtimes(session, split_command):
    penguin = session.query(Penguin).get(int(split_command[1]))

    arg_index = 2
    while True:
        if split_command[arg_index] == "ALWAYS":
            to_delete = session.query(ValidTimes).filter(ValidTimes.penguin_id == split_command[1])
            for vt in to_delete:
                session.delete(vt)
                session.commit()

            registerpenguin_whole_selected_days(session, penguin, [day for day in range(0, 7)])
            break

        else:
            determiner = None
            try:
                determiner = split_command[arg_index + 2]
            except IndexError:
                vt = session.query(ValidTimes).get((int(split_command[1]), int(split_command[arg_index + 1])))
                if vt is None:
                    registerpenguin_whole_selected_days(session, penguin, [int(split_command[arg_index + 1])])
                else:
                    vt.from_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
                    vt.to_time = datetime.datetime(1900, 1, 1, 23, 59, 0)
                    session.commit()

                break

            if determiner == "NOT":
                to_delete = session.query(ValidTimes).filter(ValidTimes.penguin_id == split_command[1],
                                                             ValidTimes.day == int(split_command[arg_index + 1]))
                for vt in to_delete:
                    session.delete(vt)
                    session.commit()
                arg_index = arg_index + 3

            elif determiner == "DAY" or determiner == "ALWAYS":
                vt = session.query(ValidTimes).get((int(split_command[1]), int(split_command[arg_index + 1])))
                if vt is None:
                    registerpenguin_whole_selected_days(session, penguin, [int(split_command[arg_index + 1])])
                else:
                    vt.from_time = datetime.datetime(1900, 1, 1, 0, 0, 0)
                    vt.to_time = datetime.datetime(1900, 1, 1, 23, 59, 0)
                    session.commit()

                arg_index = arg_index + 2
            else:
                vt = session.query(ValidTimes).get((int(split_command[1]), int(split_command[arg_index + 1])))
                from_time = split_command[arg_index + 2] + " " + split_command[arg_index + 3]
                to_time = split_command[arg_index + 4] + " " + split_command[arg_index + 5]

                from_time = datetime.datetime.strptime(from_time, "%H %M")
                to_time = datetime.datetime.strptime(to_time, "%H %M")
                if vt is None:
                    registerpenguin_selected_days_and_times(session, penguin, [[int(split_command[arg_index + 1]),
                                                                                from_time, to_time]])
                else:
                    vt.from_time = from_time
                    vt.to_time = to_time
                    session.commit()
                arg_index = arg_index + 6

        try:
            attempt = split_command[arg_index]
        except IndexError:
            break


def changefile(split_command):
    return split_command[1]


def printregistered(session, split_command, printFile):
    asc_expression = sqlalchemy.sql.expression.asc(WaitingList.registration_id)
    results = session.query(WaitingList, Penguin).join(Penguin, Penguin.penguin_id == WaitingList.penguin_id).order_by(
        asc_expression)
    print(results)

    with open(printFile, 'a') as writer:
        for i in range(0, int(split_command[1])):
            row = results[i]
            writer.write(str(row[0].registration_id) + "|" + str(row[0].penguin_id) + "|" + str(row[1].first_name) + "|"
                         + str(row[1].last_name) + "|" + str(row[1].penguin_priority) + "\n")


def printfreecenters(session, split_command, printFile):
    asc_expression = sqlalchemy.sql.expression.asc(VaccinationCenter.center_id)
    result = session.query(VaccinationCenter).filter(VaccinationCenter.district == int(split_command[1]),
                                                     VaccinationCenter.free_vaccines > 0).order_by(asc_expression)

    with open(printFile, 'a') as writer:
        for row in result:
            writer.write(str(row.center_id) + "\n")


def printvalidtimes(session, split_command, printFile):
    to_print_list = []
    arg_index = 1
    while True:
        if split_command[arg_index] == "ID":
            determiner = None
            try:
                determiner = split_command[arg_index + 2]
            except IndexError:
                to_print_list = printvalidtimes_all_penguin_times(session, int(split_command[arg_index + 1]), printFile,
                                                                  to_print_list)
                break

            if determiner == "ID":
                to_print_list = printvalidtimes_all_penguin_times(session, int(split_command[arg_index + 1]), printFile,
                                                                  to_print_list)
                arg_index = arg_index + 2
            else:  # determiner 2 je DAY, pořád nevím, který argument to je
                determiner2 = None
                try:
                    determiner2 = split_command[arg_index + 4]
                except IndexError:
                    to_print_list = printvalidtimes_selected_day_penguin_times(session,
                                                                               int(split_command[arg_index + 1]),
                                                                               int(split_command[arg_index + 3]),
                                                                               printFile, to_print_list)
                    break

                try:
                    attempt = int(determiner2)
                    to_print_list = printvalidtimes_all_penguin_times(session, int(split_command[arg_index + 1]),
                                                                      printFile, to_print_list)
                    arg_index = arg_index + 2
                except ValueError:
                    to_print_list = printvalidtimes_selected_day_penguin_times(session,
                                                                               int(split_command[arg_index + 1]),
                                                                               int(split_command[arg_index + 3]),
                                                                               printFile, to_print_list)
                    arg_index = arg_index + 4

        else:  # Arg začíná slovem DAY
            determiner = None
            try:
                determiner = split_command[arg_index + 2]
            except IndexError:
                to_print_list = printvalidtimes_all_penguins(session, int(split_command[arg_index + 1]), printFile,
                                                             to_print_list)
                break

            if determiner == "DAY" or determiner == "ID":
                to_print_list = printvalidtimes_all_penguins(session, int(split_command[arg_index + 1]), printFile,
                                                             to_print_list)
                arg_index = arg_index + 2
            else:
                from_time = split_command[arg_index + 2] + " " + split_command[arg_index + 3]
                to_time = split_command[arg_index + 4] + " " + split_command[arg_index + 5]

                from_time = datetime.datetime.strptime(from_time, "%H %M")
                to_time = datetime.datetime.strptime(to_time, "%H %M")

                to_print_list = printvalidtimes_penguins_in_selected_time(session, int(split_command[arg_index + 1]),
                                                                          from_time,
                                                                          to_time, printFile, to_print_list)
                arg_index = arg_index + 6

        try:
            attempt = split_command[arg_index]
        except IndexError:
            break

    printvalidtimes_write_from_list(to_print_list, printFile)


# --------------------------------------------------------------------------------------------------------
# SLOZITEJSI DOTAZY
# --------------------------------------------------------------------------------------------------------


def endday(session, split_command, vaccinationsLimit, printFile):
    tt1 = TimeTable(registration_id=0, vaccination_center_id=0, time=datetime.datetime(2021, 3, 21, 8, 30), penguin_id=3)
    tt2 = TimeTable(registration_id=1, vaccination_center_id=0, time=datetime.datetime(2021, 3, 21, 9, 30),
                    penguin_id=2)
    tt3 = TimeTable(registration_id=2, vaccination_center_id=2, time=datetime.datetime(2021, 3, 21, 10, 30),
                    penguin_id=4)
    session.add(tt1)
    session.add(tt2)
    session.add(tt3)
    session.commit()

    center_id_index = 1
    while True:
        try:
            center_id = int(split_command[center_id_index])
        except IndexError:
            break

        free_vaccines = int(split_command[center_id_index + 1])
        center = session.query(VaccinationCenter).get(center_id)
        center.free_vaccines = center.free_vaccines + free_vaccines
        session.commit()
        center_id_index = center_id_index + 2

    today = datetime.date.today()
    today_vaccinated_penguins = session.query(TimeTable).filter(func.date(TimeTable.time) == today)
    for record in today_vaccinated_penguins:
        penguin = session.query(Penguin).get(record.penguin_id)
        penguin.vaccine_number = penguin.vaccine_number + 1

        vc = VaccinationLog(registration_id=record.registration_id, penguin_id=record.penguin_id,
                            vaccination_number=penguin.vaccine_number, vaccination_center=record.vaccination_center_id,
                            vaccination_time=record.time)
        session.add(vc)
        session.delete(record)

        if penguin.vaccine_number == vaccinationsLimit:
            with open(printFile, 'a') as writer:
                writer.write(str(penguin.first_name) + " " + str(penguin.last_name) + " with ID: "
                             + str(penguin.penguin_id) + " is fully vaccinated!!\n")
            penguin_valid_centers = session.query(ValidCenters).filter(ValidCenters.penguin_id == penguin.penguin_id)
            for pvc in penguin_valid_centers:
                session.delete(pvc)

            penguin_valid_times = session.query(ValidTimes).filter(ValidTimes.penguin_id == penguin.penguin_id)
            for pvt in penguin_valid_times:
                session.delete(pvt)

        session.commit()

    all_centers = session.query(VaccinationCenter).order_by(VaccinationCenter.center_id)
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)

    for center in all_centers:
        # záznamy všech tučňáků, kteří chtějí do centra center a jsou na WaitingList
        prio_order = sqlalchemy.sql.expression.desc(Penguin.penguin_priority)
        reg_id_order = sqlalchemy.sql.expression.asc(WaitingList.registration_id)

        result = session.query(ValidCenters, WaitingList.registration_id, Penguin.penguin_priority) \
            .join(WaitingList, WaitingList.penguin_id == ValidCenters.penguin_id) \
            .join(Penguin, Penguin.penguin_id == ValidCenters.penguin_id) \
            .filter(ValidCenters.center_id == center.center_id).order_by(prio_order, reg_id_order)

        counter = 0
        for row in result:
            if counter == center.free_vaccines:
                break
            peng = session.query(Penguin).get(row[0].penguin_id)
            print()
            print("tucnak", peng)

            find_and_set_date(session, row, center.work_from, center.work_till, tomorrow)
            counter = counter + 1


def findappointments(session, split_command, printFile):
    result_list = []
    arg_index = 1
    print()
    while True:
        if split_command[arg_index] == "ID":
            peng_id = int(split_command[arg_index + 1])
            penguin_times = session.query(TimeTable).filter(TimeTable.penguin_id == peng_id)
            for result in penguin_times:
                item = (result.registration_id, result.vaccination_center_id, result.time)
                print(item)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 2

        elif split_command[arg_index] == "DATE":
            time_str = split_command[arg_index + 1] + " " + split_command[arg_index + 2] + " " + split_command[
                arg_index + 3]
            when = datetime.datetime.strptime(time_str, "%Y %m %d").date()
            all_penguins = session.query(TimeTable).filter(func.date(TimeTable.time) == when)
            for result in all_penguins:
                item = (result.registration_id, result.vaccination_center_id, result.time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 4

        elif split_command[arg_index] == "CENTER":
            center_id = int(split_command[arg_index + 1])
            penguin_times_in_center = session.query(TimeTable).filter(TimeTable.vaccination_center_id == center_id)
            for result in penguin_times_in_center:
                item = (result.registration_id, result.vaccination_center_id, result.time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 2

        else:
            center_id = int(split_command[arg_index + 1])
            time_str = split_command[arg_index + 2] + " " + split_command[arg_index + 3] + " " + split_command[
                arg_index + 4]
            when = datetime.datetime.strptime(time_str, "%Y %m %d").date()
            all_penguin_times_in_center = session.query(TimeTable).filter(TimeTable.vaccination_center_id == center_id)\
                .filter(func.date(TimeTable.time) == when)
            for result in all_penguin_times_in_center:
                item = (result.registration_id, result.vaccination_center_id, result.time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 5

        try:
            attempt = split_command[arg_index]
        except IndexError:
            break

    result_list = sorted(result_list, key=itemgetter(2, 0))
    with open(printFile, 'a') as writer:
        for record in result_list:
            writer.write(str(record[0]) + "|" + str(record[1]) + "|" + str(record[2]) + "\n")


def findloggedvaccinations(session, split_command, printFile):
    result_list = []
    arg_index = 1
    while True:
        if split_command[arg_index] == "ID":
            peng_id = int(split_command[arg_index + 1])
            penguin_times = session.query(VaccinationLog).filter(VaccinationLog.penguin_id == peng_id)

            for result in penguin_times:
                item = (result.penguin_id, result.registration_id, result.vaccination_number,
                        result.vaccination_center, result.vaccination_time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 2

        elif split_command[arg_index] == "DATE":
            time_str = split_command[arg_index + 1] + " " + split_command[arg_index + 2] + " " \
                       + split_command[arg_index + 3]
            when = datetime.datetime.strptime(time_str, "%Y %m %d").date()
            all_penguins = session.query(VaccinationLog).filter(func.date(VaccinationLog.vaccination_time) == when)

            for result in all_penguins:
                item = (result.penguin_id, result.registration_id, result.vaccination_number,
                        result.vaccination_center, result.vaccination_time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 4

        elif split_command[arg_index] == "CENTER":
            center_id = int(split_command[arg_index + 1])
            penguins_in_center = session.query(VaccinationLog).filter(VaccinationLog.vaccination_center == center_id)

            for result in penguins_in_center:
                item = (result.penguin_id, result.registration_id, result.vaccination_number,
                        result.vaccination_center, result.vaccination_time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 2

        elif split_command[arg_index] == "CENTERDATE":
            center_id = int(split_command[arg_index + 1])
            time_str = split_command[arg_index + 2] + " " + split_command[arg_index + 3] + " " + split_command[
                arg_index + 4]
            when = datetime.datetime.strptime(time_str, "%Y %m %d").date()
            all_penguins_in_time = session.query(VaccinationLog).filter(VaccinationLog.vaccination_center == center_id) \
                .filter(func.date(VaccinationLog.vaccination_time) == when)

            for result in all_penguins_in_time:
                item = (result.penguin_id, result.registration_id, result.vaccination_number,
                        result.vaccination_center, result.vaccination_time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 5

        else:
            level = int(split_command[arg_index + 1])
            all_penguins_with_level = session.query(VaccinationLog) \
                .filter(VaccinationLog.vaccination_number == level)

            for result in all_penguins_with_level:
                item = (result.penguin_id, result.registration_id, result.vaccination_number,
                        result.vaccination_center, result.vaccination_time)
                if item not in result_list:
                    result_list.append(item)
            arg_index = arg_index + 2

        try:
            attempt = split_command[arg_index]
        except IndexError:
            break

    result_list = sorted(result_list, key=itemgetter(0, 1))
    with open(printFile, 'a') as writer:
        for record in result_list:
            writer.write(str(record[0]) + "|" + str(record[1]) + "|" + str(record[2]) + "|" + str(record[3]) +
                         "|" + str(record[4]) + "\n")


def givestatistics(session, printFile, vaccinationsLimit):
    count = session.query(func.count(distinct(VaccinationLog.penguin_id)))

    best_vaccinated = session.query(Penguin.district, func.count(Penguin.district), Penguin.vaccine_number) \
        .filter(Penguin.vaccine_number == vaccinationsLimit).group_by(Penguin.district).order_by(
        desc(func.count(Penguin.district))).first()
    best_vaccinated_dist = best_vaccinated[0]

    most_vaccinated = session.query(VaccinationLog.vaccination_center, VaccinationCenter.district, func.count(VaccinationCenter.district))\
        .join(VaccinationCenter, VaccinationCenter.center_id == VaccinationLog.vaccination_center).group_by(VaccinationCenter.district).order_by(
        desc(func.count(VaccinationCenter.district))).first()
    most_vaccinated_dist = most_vaccinated[0]

    most_working = session.query(VaccinationLog.vaccination_center, func.count(VaccinationLog.vaccination_center))\
        .group_by(VaccinationLog.vaccination_center).order_by(desc(func.count(VaccinationLog.vaccination_center))).first()
    most_working_center = most_working[0]

    first_fully_vaccinated = session.query(VaccinationLog.penguin_id, VaccinationLog.vaccination_number, VaccinationLog.vaccination_time)\
        .filter(VaccinationLog.vaccination_number == vaccinationsLimit).order_by(VaccinationLog.vaccination_time).first()[0]

    favourite_day_from_vl = session.query(func.date(VaccinationLog.vaccination_time), func.count(func.date(VaccinationLog.vaccination_time)))\
        .group_by(func.date(VaccinationLog.vaccination_time)).order_by(desc(func.count(func.date(VaccinationLog.vaccination_time)))).first()

    favourite_day_from_tt = session.query(func.date(TimeTable.time), func.count(func.date(TimeTable.time))) \
        .group_by(func.date(TimeTable.time)).order_by(desc(func.count(func.date(TimeTable.time)))).first()

    date_to_print = None
    if favourite_day_from_vl[1] == favourite_day_from_tt[1]:
        date_to_print = favourite_day_from_vl[0]

    elif favourite_day_from_vl[1] > favourite_day_from_tt[1]:
        date_to_print = favourite_day_from_vl[0]

    else:
        date_to_print = favourite_day_from_tt[0]

    monday = [1, 0]
    tuesday = [2, 0]
    wednesday = [3, 0]
    thursday = [4, 0]
    friday = [5, 0]
    saturday = [6, 0]
    sunday = [0, 0]

    weekdays_from_vl = session.query(func.extract('dow', VaccinationLog.vaccination_time),
                                              func.count(func.extract('dow', VaccinationLog.vaccination_time))) \
        .group_by(func.extract('dow', VaccinationLog.vaccination_time)) \
        .order_by(desc(func.count(func.extract('dow', VaccinationLog.vaccination_time)))).all()

    monday, tuesday, wednesday, thursday, friday, saturday, sunday = count_weekdays(weekdays_from_vl, monday, tuesday,
                                                                                    wednesday, thursday, friday,
                                                                                    saturday, sunday)

    weekdays_from_tt = session.query(func.extract('dow', TimeTable.time),
                                              func.count(func.extract('dow', TimeTable.time))) \
        .group_by(func.extract('dow', TimeTable.time)) \
        .order_by(desc(func.count(func.extract('dow', TimeTable.time))))

    monday, tuesday, wednesday, thursday, friday, saturday, sunday = count_weekdays(weekdays_from_tt, monday, tuesday,
                                                                                    wednesday, thursday, friday,
                                                                                    saturday, sunday)

    weekday_to_print = max(monday, tuesday, wednesday, thursday, friday, saturday, sunday, key=itemgetter(1))
    if weekday_to_print[0] == 0:
        weekday_to_print[0] = 6
    else:
        weekday_to_print[0] = weekday_to_print[0] - 1

    with open(printFile, 'a') as writer:
        writer.write("So far " + str(count[0][0]) + " of penguins have been vaccinated.\n")
        writer.write("Best vaccinated district " + str(best_vaccinated_dist) + "\n")
        writer.write("Favourite district " + str(most_vaccinated_dist) + "\n")
        writer.write("Favourite center " + str(most_working_center) + "\n")
        writer.write("First fully vaccinated " + str(first_fully_vaccinated) + "\n")
        writer.write("Favourite day " + str(date_to_print) + "\n")
        writer.write("Favourite weekday " + str(weekday_to_print[0]) + "\n")


# --------------------------------------------------------------------------------------------------------
# POMOCNE FUNKCE
# --------------------------------------------------------------------------------------------------------

def count_weekdays(table, monday, tuesday, wednesday, thursday, friday, saturday, sunday):
    """Funkce slouží pro počítání nejoblíbenějšího dne v týdnu pro očkování"""
    for record in table:
        if record[0] == 1:
            monday[1] = monday[1] + record[1]
        elif record[0] == 2:
            tuesday[1] = tuesday[1] + record[1]
        elif record[0] == 3:
            wednesday[1] = wednesday[1] + record[1]
        elif record[0] == 4:
            thursday[1] = thursday[1] + record[1]
        elif record[0] == 5:
            friday[1] = friday[1] + record[1]
        elif record[0] == 6:
            saturday[1] = saturday[1] + record[1]
        elif record[0] == 0:
            sunday[1] = sunday[1] + record[1]

    return monday, tuesday, wednesday, thursday, friday, saturday, sunday


def calc_penguin_prio(penguin_age, old_age, vaccinations_limit, vaccine_number, medic):
    """Spočítá prioritu pro daného tučňáka"""
    prio = (vaccine_number + 1) * 4
    if medic == 1 and penguin_age <= (old_age * 365):
        prio = prio - 1
    elif medic != 1 and penguin_age > (old_age * 365):
        prio = prio - 2
    elif medic != 1 and penguin_age <= (old_age * 365):
        prio = prio - 3

    return prio


def registerpenguin_all_centres(session, penguin):
    """Funkce přidá všechna centra otevřená právě teď jako ValidCenters tučňáka Penguin"""
    result = session.query(VaccinationCenter).filter(VaccinationCenter.district == penguin.district)
    for center in result:
        if center.work_from.time() <= datetime.datetime.now().time() <= center.work_till.time():
            vc = ValidCenters(penguin_id=penguin.penguin_id, center_id=center.center_id)
            session.add(vc)
            session.commit()


def registerpenguin_selected_centres(session, penguin, split_command, start_index):
    """Funkce přidá centra v příkazu jako ValidCenters pro daného tučňáka. Pokud žádné ze zadaných neexistuje, přidá
    všechna právě otevřená"""
    one_exists = False
    i = 1
    continue_index = 0
    while True:
        try:
            center_id = int(split_command[start_index + i])
            if session.query(VaccinationCenter).get(center_id) is not None:
                one_exists = True
                vc = ValidCenters(penguin_id=penguin.penguin_id, center_id=center_id)
                session.add(vc)
                session.commit()

        except ValueError:
            continue_index = start_index + i
            if not one_exists:
                registerpenguin_all_centres(session, penguin)
            break

        i = i + 1
    return continue_index


def registerpenguin_whole_selected_days(session, penguin, days_list):
    """Nastaví ValidTimes pro daného tučňáka od 0:00 do 23:59 ve vybrané dny (days_list)"""
    for d in days_list:
        vt = ValidTimes(penguin_id=penguin.penguin_id, day=d, from_time=datetime.datetime(1900, 1, 1, 0, 0, 0),
                        to_time=datetime.datetime(1900, 1, 1, 23, 59, 0))
        session.add(vt)
        session.commit()


def registerpenguin_selected_days_and_times(session, penguin, days_and_times_list):
    """Nastaví ValidTimes pro daného tučňáka ve vybrané dny a časové intervaly (days_and_times_list)"""
    for tup in days_and_times_list:
        vt = ValidTimes(penguin_id=penguin.penguin_id, day=tup[0], from_time=tup[1],
                        to_time=tup[2])
        session.add(vt)
        session.commit()


def printvalidtimes_all_penguin_times(session, penguin_id, printFile, to_print_list):
    """Funkce vrátí všechny časy všech dnů tučňáka"""
    result = session.query(ValidTimes).filter(ValidTimes.penguin_id == penguin_id).order_by(ValidTimes.penguin_id,
                                                                                            ValidTimes.day)
    return printvalidtimes_add_to_list_from_table(result, printFile, to_print_list)


def printvalidtimes_selected_day_penguin_times(session, penguin_id, day, printFile, to_print_list):
    """Funkce vrátí všechny časy tučňáka v daný den"""
    result = session.query(ValidTimes).filter(ValidTimes.penguin_id == penguin_id, ValidTimes.day == day) \
        .order_by(ValidTimes.penguin_id, ValidTimes.day)

    return printvalidtimes_add_to_list_from_table(result, printFile, to_print_list)


def printvalidtimes_all_penguins(session, day, printFile, to_print_list):
    """Funkce vrátí všechny tučňáky, kteří mohou v daný den"""
    result = session.query(ValidTimes).filter(ValidTimes.day == day).order_by(ValidTimes.penguin_id, ValidTimes.day)
    return printvalidtimes_add_to_list_from_table(result, printFile, to_print_list)


def printvalidtimes_penguins_in_selected_time(session, day, from_time, to_time, printFile, to_print_list):
    """Funkce vrátí všechny tučňáky, kteří mohou v daný den v časovém intervalu mezi from_time a to_time"""
    result = session.query(ValidTimes).filter(ValidTimes.day == day) \
        .filter(ValidTimes.from_time >= from_time, ValidTimes.to_time <= to_time) \
        .order_by(ValidTimes.penguin_id, ValidTimes.day)

    return printvalidtimes_add_to_list_from_table(result, printFile, to_print_list)


def printvalidtimes_add_to_list_from_table(table, printFile, to_print_list):
    """Funkce přidá všechny záznamy z tabulky table do seznamu to_print_list, který vrátí"""
    for row in table:
        record = str(row.penguin_id) + "|" + str(row.day) + "|" + str(row.from_time.time()) + "|" + str(
            row.to_time.time()) + "\n"
        if record not in to_print_list:
            to_print_list.append(record)

    return to_print_list


def printvalidtimes_write_from_list(records_list, printFile):
    """Funkce vypíše všechny záznamy ze seznamu records_list"""
    with open(printFile, 'a') as writer:
        for record in records_list:
            writer.write(record)


def find_and_set_date(session, record, work_from, work_till, today):
    """Funkce najde a nastaví termín pro daného tučňáka pokud to lze

        :param work_from: začátek pracovní doby centra
        :param work_till: konec pracovní doby centra
    """
    # všechny časy daného tučňáka serazene podle dnu
    vt_peng = session.query(ValidTimes).filter(ValidTimes.penguin_id == record[0].penguin_id).order_by(ValidTimes.day)

    # interval je vlastne prunik 2 casovych intervalu; pracovni doby centra a tucnakovy preference
    interval_from = None    # zacatek casoveho intervalu, od kdy se tucnak muze nechat naockovat
    interval_to = None  # konec casoveho intervalu, do kdy se tucnak muze nechat naockovat

    # seznam uchovava casove intervaly, kdy se tucnak v dany den chce nechat naockovat
    intervals_list = []
    for row in vt_peng:
        # zjisti interval, ve kterem tucnak muze jit na ockovani
        if row.from_time >= work_from and row.to_time <= work_till:
            interval_from = row.from_time
            interval_to = row.to_time
        elif work_till >= row.from_time >= work_from and row.to_time > work_till:
            interval_from = row.from_time
            interval_to = work_till
        elif row.from_time < work_from and work_from <= row.to_time <= work_till:
            interval_from = work_from
            interval_to = row.to_time
        else:
            continue  # intervaly se nikde neprotinaji => neni mozne tucnaka v tento den naockovat

        if (interval_to - interval_from).seconds < 540:
            continue  # casovy interval na naockovani neni 10 minut => neni mozne tucnaka v tento den naockovat

        intervals_list.append((row.day, interval_from, interval_to))

    if intervals_list == []:
        return False    # tucnaka nelze naockovat, protoze nema zadny casovy interval, kdy by to bylo mozne

    index = 0
    number_of_iterations = 0    # v jakem tydnu se maji hledat vhodne dny pro vakcinaci
    while True:     # tento cyklus je zde proto, ze vsechny vhodne terminy mohou byt v dany tyden obsazene => je potreba prejit do dalsiho
        # den, ve ktery se tucnak chce nechat oockovat
        chosen_day = today

        start = number_of_iterations * 7
        end = start + 7
        for i in range(start, end):
            chosen_day = today + datetime.timedelta(days=i)

            if chosen_day.weekday() == intervals_list[index][0]:
                break

        # vyfiltruj vsechny terminy pro dany den
        tt = session.query(TimeTable.time).filter(func.date(TimeTable.time) == chosen_day).order_by(TimeTable.time)
        intended_time = datetime.datetime(chosen_day.year, chosen_day.month, chosen_day.day,
                                          intervals_list[index][1].hour, intervals_list[index][1].minute)

        while True:
            result = is_time_valid(tt, intended_time)
            if result[0]:
                new_time = TimeTable(registration_id=record[1], vaccination_center_id=record[0].center_id,
                                     time=intended_time, penguin_id=record[0].penguin_id)
                session.add(new_time)
                session.delete(session.query(WaitingList).get(record[1]))
                center = session.query(VaccinationCenter).get(record[0].center_id)
                center.free_vaccines = center.free_vaccines - 1
                session.commit()
                return True
            else:
                intended_time = result[1]

            if intended_time.hour >= intervals_list[index][2].hour and intended_time.minute >= intervals_list[index][
                2].minute:

                break

        if index == len(intervals_list) - 1:
            index = 0
            number_of_iterations = number_of_iterations + 1
        else:
            index = index + 1


def is_time_valid(tt, intended_time):
    """Zjistí, zda plánovaný očkovací termín nezasahuje do naplanovanych terminu pro tento den z tabulky TimeTable
    a vrátí konec naplanovaneho terminu v pripade, ze zasahuje do naplanovaneho terminu"""

    intended_time_till = intended_time + datetime.timedelta(minutes=9)

    for time in tt:
        time_till = time[0] + datetime.timedelta(minutes=9)
        if time_till >= intended_time >= time[0] or time[0] <= intended_time_till <= time_till:
            return False, time_till + datetime.timedelta(minutes=1)

    return True, None
