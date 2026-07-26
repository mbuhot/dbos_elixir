alias LiveApprovals.Reviews

{:ok, _small} =
  Reviews.submit(%{
    "title" => "Taxi to the airport",
    "amount_cents" => 4_200,
    "submitter" => "dana"
  })

{:ok, _large} =
  Reviews.submit(%{
    "title" => "Team offsite dinner",
    "amount_cents" => 45_000,
    "submitter" => "dana"
  })
