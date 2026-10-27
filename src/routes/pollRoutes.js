import express from "express";
import { middleware } from "../middlewares/middleware.js";
import {
  createPoll,
  getPoll,
  listPolls,
  voteForPoll,
} from "../controller/poll.conttroller.js";

const route = express.Router();

route.get("/", listPolls);
route.post("/", middleware, createPoll);
route.post("/create", middleware, createPoll);
route.get("/:pollId", getPoll);
route.post("/:pollId/votes", middleware, voteForPoll);

export default route;