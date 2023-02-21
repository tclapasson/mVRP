from ortools import *
from ortools.constraint_solver import pywrapcp
from ortools.constraint_solver import routing_enums_pb2


def create_data_model(nb_vehicle, matrice, ordre):
    """Stores the data for the problem."""
    data = {'distance_matrix': matrice, 'num_vehicles': nb_vehicle, 'starts': ordre[0], 'ends': ordre[0]}
    routage = {}
    return data, routage


def print_solution(data, manager, routing, solution, routage):
    """Prints solution on console."""
    print(f'Objective: {solution.ObjectiveValue()}')
    max_route_distance = 0
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        plan_output = 'Route for vehicle {}'.format(vehicle_id)
        route_distance = 0
        routage['{}'.format(vehicle_id)] = [data['starts'][vehicle_id]]
        while not routing.IsEnd(index):
            plan_output += ' {} -> '.format(manager.IndexToNode(index))
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id)
            routage['{}'.format(vehicle_id)].append(manager.IndexToNode(index))
        plan_output += '{}\n'.format(manager.IndexToNode(index))
        plan_output += 'Cout de la route: {}\n'.format(route_distance)
        print(plan_output)
        max_route_distance = max(route_distance, max_route_distance)
    print('Cout maximum de la route: {}'.format(max_route_distance))


def mvrp(nb_vehicle, matrice, ordre, pointIdcontrat):
    """Entry point of the program."""
    # Instantiate the data problem.
    data, routage = create_data_model(nb_vehicle, matrice, ordre)

    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']), data['num_vehicles'], data['starts'],
                                           data['ends'])

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)

    # Create and register a transit callback.
    def distance_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        # Convert from routing variable Index to distance matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)

    ############ Ajout de la contraite = Points même numero de contrat visites par le même vehicules

    contrainteContratsVehicule = True

    if contrainteContratsVehicule:
        for i in range(len(pointIdcontrat)):
            routing.AddSoftSameVehicleConstraint(pointIdcontrat[i],
                                                 100000000)  ### 10000000000 represente le coût si on ne peut pas respecter la contrainte, il est volontairement tres grand pour ne pas pouvoir enfreindre la contrainte

    # Define cost of each arc.
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add Distance constraint.
    dimension_name = 'Distance'
    routing.AddDimension(
        transit_callback_index,
        0,  # no slack
        10000,  # vehicle maximum travel time
        True,  # start cumul to zero
        dimension_name)
    distance_dimension = routing.GetDimensionOrDie(dimension_name)
    distance_dimension.SetGlobalSpanCostCoefficient(100)

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC)
    search_parameters.time_limit.seconds = 20

    # Solve the problem.
    solution = routing.SolveWithParameters(search_parameters)

    # Print solution on console.
    if solution:
        print_solution(data, manager, routing, solution, routage)
        return routage
    else:
        print('No solution found !')
