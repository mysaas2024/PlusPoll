import express from "express";
import { Login, Register, resetPassword } from "../controller/authcontroller.js";

const route = express.Router();

route.get("/health", (_req, res) => {
  res.status(200).json({ success: true, message: "Auth API is working." });
});
route.post("/register", Register);
route.post("/login", Login);
route.post("/reset-password", resetPassword);

export default route;