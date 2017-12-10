class TicketKiosk:
    def __init__(self):
        self.tickets_available = 100

    def sell_tickets(self, num_of_tickets):
        self.tickets_available -= num_of_tickets

    def get_available_tickets(self):
        return self.tickets_available

