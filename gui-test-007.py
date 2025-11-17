# file: app/trading_control_app.py
from dataclasses import dataclass
from typing import Optional, Dict
import customtkinter as ctk #

# ---- Appearance ----
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

PRIMARY_BTN_BG = "#224e94"
PRIMARY_BTN_BG_HOVER = "#4287f5"
PRIMARY_BTN_TEXT = "#FFFFFF"

ACCENT_FG = "#285d66"
ACCENT_FG_DARK = "#4fe5ff"
ACCENT_BORDER = "#253240"

TEXT_PRIMARY = "#FFFFFF"
TEXT_MUTED = "#C9C9C9"
BG_DARK = "#1A1A1A"
FIELD_BG = "#2B2B2B"
FIELD_BORDER = "#404040"

FONT_BASE = ("Inter", 16)


@dataclass(frozen=True)
class SliderSpec:
    from_: float = 0.0
    to: float = 100.0
    number_of_steps: Optional[int] = 100
    initial: float = 0.0


def _clip(v: float, lo: float, hi: float) -> float:
    return min(max(v, lo), hi)


def _step_precision(spec: SliderSpec) -> int:
    if not spec.number_of_steps or spec.number_of_steps <= 0:
        return 2
    step = (spec.to - spec.from_) / spec.number_of_steps
    if step <= 0:
        return 2
    for decimals in range(0, 6):
        if round(step, decimals) == step or round(step * (10 ** decimals)) % 1 == 0:
            return decimals
    return 3


class TradingControlApp(ctk.CTk):
    """Per-slider specs; Manual Stop with Set + read-only badge; footer Exit."""
    def __init__(self, slider_specs: Optional[Dict[str, SliderSpec]] = None) -> None:
        super().__init__()
        self.title("Trading Controls")
        self.geometry("400x400")
        self.resizable(False, False)
        self.configure(fg_color=BG_DARK)

        default_specs = {
            "quantity": SliderSpec(from_=1, to=3, number_of_steps=2, initial=1),
            "initial_stop": SliderSpec(from_=20, to=90, number_of_steps=14, initial=75),
            "first_target": SliderSpec(from_=100, to=300, number_of_steps=40, initial=150),
        }
        if slider_specs:
            default_specs.update(slider_specs)
        self.slider_specs = default_specs

        self.option_side = ctk.StringVar(value="CALL")
        self.manual_stop_enabled = ctk.BooleanVar(value=False)
        self.manual_stop_text = ctk.StringVar(value="7.05")
        self.manual_stop_level: Optional[float] = None  # why: shows applied level on the badge

        self.quantity_var = ctk.DoubleVar(value=_clip(
            self.slider_specs["quantity"].initial,
            self.slider_specs["quantity"].from_,
            self.slider_specs["quantity"].to,
        ))
        self.initial_stop_var = ctk.DoubleVar(value=_clip(
            self.slider_specs["initial_stop"].initial,
            self.slider_specs["initial_stop"].from_,
            self.slider_specs["initial_stop"].to,
        ))
        self.first_target_var = ctk.DoubleVar(value=_clip(
            self.slider_specs["first_target"].initial,
            self.slider_specs["first_target"].from_,
            self.slider_specs["first_target"].to,
        ))

        self.columnconfigure(0, weight=1)

        self._build_header()
        self._build_quantity_section()
        self._build_initial_stop_section()
        self._build_first_target_section()
        self._build_manual_stop_section()
        self._build_footer()

        self._refresh_set_button_state()
        self._update_manual_stop_badge()

    # ---------------- Sections ----------------
    def _build_header(self) -> None:
        frame = ctk.CTkFrame(self, fg_color="transparent")
        frame.grid(row=0, column=0, sticky="ew", padx=16, pady=(20, 8))
        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        buy_btn = ctk.CTkButton(
            frame,
            text="Buy",
            width=100,
            height=40,
            fg_color=PRIMARY_BTN_BG,
            hover_color=PRIMARY_BTN_BG_HOVER,
            text_color=PRIMARY_BTN_TEXT,
            corner_radius=8,
            font=FONT_BASE,
            command=self.on_buy,
        )
        buy_btn.grid(row=0, column=0, sticky="w")

        side_frame = ctk.CTkFrame(frame, fg_color="transparent")
        side_frame.grid(row=0, column=1, sticky="e")

        for i, (text, val) in enumerate([("Call", "CALL"), ("Put", "PUT")]):
            rb = ctk.CTkRadioButton(
                side_frame,
                text=text,
                variable=self.option_side,
                value=val,
                fg_color=ACCENT_FG,
                text_color=TEXT_PRIMARY,
                border_color=ACCENT_BORDER,
                command=self.on_option_change,
            )
            rb.grid(row=0, column=i, padx=(0, 8) if i == 0 else 0)

    def _build_quantity_section(self) -> None:
        self.quantity_frame, self.quantity_value_lbl = self._section_with_slider(
            row=1,
            title="Quantity",
            var=self.quantity_var,
            spec=self.slider_specs["quantity"],
        )

    def _build_initial_stop_section(self) -> None:
        self.initial_stop_frame, self.initial_stop_value_lbl = self._section_with_slider(
            row=2,
            title="Init. Stop",
            var=self.initial_stop_var,
            spec=self.slider_specs["initial_stop"],
        )

    def _build_first_target_section(self) -> None:
        self.first_target_frame, self.first_target_value_lbl = self._section_with_slider(
            row=3,
            title="1st Target",
            var=self.first_target_var,
            spec=self.slider_specs["first_target"],
        )

    def _build_manual_stop_section(self) -> None:
        frame = ctk.CTkFrame(self, fg_color="transparent")
        frame.grid(row=4, column=0, sticky="ew", padx=16, pady=(20, 8))
        frame.columnconfigure(0, weight=0)
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(2, weight=0)

        enable_cb = ctk.CTkCheckBox(
            frame,
            text="Enable:",
            fg_color=ACCENT_FG,
            text_color=TEXT_MUTED,
            border_color=ACCENT_BORDER,
            variable=self.manual_stop_enabled,
            command=self.on_manual_toggle,  # why: toggles Edit/Set availability
        )
        enable_cb.grid(row=0, column=0, sticky="w")

        self.manual_stop_entry = ctk.CTkEntry(
            frame,
            placeholder_text="7.05",
            width=120,
            height=40,
            fg_color=FIELD_BG,
            text_color=TEXT_PRIMARY,
            border_width=6.74,
            corner_radius=8,
            border_color=FIELD_BORDER,
            font=FONT_BASE,
            textvariable=self.manual_stop_text,
            state="disabled",
        )
        self.manual_stop_entry.grid(row=0, column=1, sticky="ew", padx=(8, 8))

        self.manual_stop_set_btn = ctk.CTkButton(
            frame,
            text="Set",
            width=70,
            height=40,
            fg_color=PRIMARY_BTN_BG,
            hover_color=PRIMARY_BTN_BG_HOVER,
            text_color=PRIMARY_BTN_TEXT,
            corner_radius=8,
            font=FONT_BASE,
            command=self.on_set_manual_stop,  # why: commit manual stop
            state="disabled",
        )
        self.manual_stop_set_btn.grid(row=0, column=2, sticky="e")

        # Badge row -> RIGHT aligned
        badge_frame = ctk.CTkFrame(frame, fg_color="transparent")
        badge_frame.grid(row=1, column=0, columnspan=3, sticky="e", pady=(10, 0))
        badge_frame.columnconfigure(0, weight=1)

        self.manual_stop_badge = ctk.CTkLabel(
            badge_frame,
            text="Manual Stop: —",
            text_color=TEXT_PRIMARY,
            font=("Inter", 14),
            fg_color=FIELD_BG,   # pill look
            corner_radius=12,
            padx=10,
            pady=6,
        )
        self.manual_stop_badge.pack(anchor="e")  # right side

        # Validate on every change
        self.manual_stop_text.trace_add("write", lambda *_: self._refresh_set_button_state())

    def _build_footer(self) -> None:
        footer = ctk.CTkFrame(self, fg_color="transparent")
        footer.grid(row=5, column=0, sticky="ew", padx=16, pady=(8, 16))
        footer.columnconfigure(0, weight=0)
        footer.columnconfigure(1, weight=1)

        exit_btn = ctk.CTkButton(
            footer,
            text="Exit",
            width=150,
            height=60,
            fg_color="#321a45",
            hover_color="#4b2866",
            text_color=TEXT_PRIMARY,
            corner_radius=8,
            font=FONT_BASE,
            command=self.on_exit,  # stand-in exit command
        )
        exit_btn.grid(row=0, column=0, sticky="w")

    # --------------- Helpers ------------------
    def _section_with_slider(
        self,
        row: int,
        title: str,
        var: ctk.DoubleVar,
        spec: SliderSpec,
    ):
        frame = ctk.CTkFrame(self, fg_color="transparent")
        frame.grid(row=row, column=0, sticky="ew", padx=16, pady=8)
        frame.columnconfigure(0, weight=0)
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(2, weight=0)

        label = ctk.CTkLabel(frame, text=title, text_color=TEXT_PRIMARY, font=FONT_BASE)
        label.grid(row=0, column=0, sticky="w")

        precision = _step_precision(spec)
        slider = ctk.CTkSlider(
            frame,
            from_=spec.from_,
            to=spec.to,
            number_of_steps=spec.number_of_steps,
            fg_color=ACCENT_FG,
            progress_color=ACCENT_FG_DARK,
            border_color=ACCENT_BORDER,
            command=lambda v, p=precision, lbl=None: self._on_slider_change(v, p, value_lbl),
            variable=var,
        )
        slider.grid(row=0, column=1, sticky="ew", padx=12)

        value_lbl = ctk.CTkLabel(
            frame,
            text=self._format_value(var.get(), precision),
            text_color=TEXT_PRIMARY,
            font=FONT_BASE,
        )
        value_lbl.grid(row=0, column=2, sticky="e")

        slider.configure(command=lambda v, p=precision, lbl=value_lbl: self._on_slider_change(v, p, lbl))

        return frame, value_lbl

    def _format_value(self, value: float, precision: int) -> str:
        fmt = f"{{:.{precision}f}}" if precision > 0 else "{:.0f}"
        return fmt.format(float(value))

    def _parse_manual_stop(self) -> Optional[float]:
        """Why: ensure Set button & badge only accept numeric."""
        txt = self.manual_stop_text.get().strip()
        if not txt:
            return None
        try:
            return float(txt)
        except ValueError:
            return None

    def _refresh_set_button_state(self) -> None:
        enabled = self.manual_stop_enabled.get() and (self._parse_manual_stop() is not None)
        self.manual_stop_set_btn.configure(state=("normal" if enabled else "disabled"))

    def _update_manual_stop_badge(self) -> None:
        """Why: reflect applied value clearly; show em dash if none."""
        if self.manual_stop_level is None:
            self.manual_stop_badge.configure(text="Manual Stop: —")
        else:
            self.manual_stop_badge.configure(text=f"Manual Stop: {self.manual_stop_level}")

    # --------------- Callbacks ----------------
    def _on_slider_change(self, value: float, precision: int, label_widget: ctk.CTkLabel) -> None:
        label_widget.configure(text=self._format_value(value, precision))

    def on_manual_toggle(self) -> None:
        state = "normal" if self.manual_stop_enabled.get() else "disabled"
        self.manual_stop_entry.configure(state=state)
        self._refresh_set_button_state()

    def on_option_change(self) -> None:
        print(f"Option side changed -> {self.option_side.get()}")

    def on_set_manual_stop(self) -> None:
        """Apply manual stop from entry and update badge."""
        val = self._parse_manual_stop()
        if val is None:
            return
        self.manual_stop_level = val
        self._update_manual_stop_badge()
        print({"manual_stop_set_to": self.manual_stop_level})

    def on_buy(self) -> None:
        def maybe_int(val: float, spec: SliderSpec):
            prec = _step_precision(spec)
            return int(round(val)) if prec == 0 else round(val, prec)

        payload = {
            "side": self.option_side.get(),
            "quantity": maybe_int(self.quantity_var.get(), self.slider_specs["quantity"]),
            "initial_stop": maybe_int(self.initial_stop_var.get(), self.slider_specs["initial_stop"]),
            "first_target": maybe_int(self.first_target_var.get(), self.slider_specs["first_target"]),
            "manual_stop": self.manual_stop_level if self.manual_stop_enabled.get() else None,
        }
        print(payload)

    def on_exit(self) -> None:
        """Stand-in exit; replace with real shutdown later."""
        print("Exit pressed")  # why: placeholder action


if __name__ == "__main__":
    app = TradingControlApp()
    app.mainloop()
